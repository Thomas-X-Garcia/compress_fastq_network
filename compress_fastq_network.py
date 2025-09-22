#!/usr/bin/env python3
"""
Three-stage decoupled pipeline for safe compression of files stored on network filesystems.

Behavior: IN-PLACE compression (files land exactly where found)

Input Modes:
  1. Directory mode (default): Provide a text file listing directories
  2. File list mode: Use --file-list with a text file listing specific files

Pipeline architecture:
  Stage 1: Downloader (1 thread) - Sequential downloads to temp
  Stage 2: Compressor (1 thread) - Sequential compression with pigz
  Stage 3: Uploader (1 thread) - Sequential uploads to destination

Decoupled design maximizes throughput:
- Compression happens locally to prevent network corruption
- Upload uses atomic operations for data integrity
- Checksums verify data at every transfer boundary

Strict concurrency limits:
- Exactly one file downloading at any time
- Exactly one pigz process at any time
- Exactly one file uploading at any time
- Maximum parallel I/O: 1 download + 1 upload

Features:
- Non-overwrite policy: skip if destination .gz exists
- Recursive discovery in fastq_pass/fastq_fail directories only
- Atomic uploads with race condition handling
- Checksum verification at each transfer boundary
- Two bounded queues for backpressure control
- Temp disk space management
- Clean shutdown on signals
- Pipeline efficiency statistics
"""

import argparse
import errno
import hashlib
import logging
import os
import queue
import select
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Event, Thread, Lock
from typing import Optional, List, Tuple, Dict

# ============================================================================
# Configuration Constants
# ============================================================================

# Default values
DEFAULT_FILE_EXTENSIONS = ['.fastq']  # Default extensions to compress
DEFAULT_CHECKSUM_ALGO = 'sha256'
DEFAULT_COMPRESS_QUEUE_SIZE = 0  # 0 means unlimited
DEFAULT_UPLOAD_QUEUE_SIZE = 0    # 0 means unlimited
DEFAULT_RETRIES = 3
DEFAULT_RETRY_WAIT = 3
DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024  # 8MB
DEFAULT_NETWORK_TIMEOUT = 60  # 60 seconds - enough for network issues to manifest

# Directory names to search within
FASTQ_DIRECTORY_NAMES = {'fastq_pass', 'fastq_fail'}

# Checksum algorithms
CHECKSUM_ALGORITHMS = {
    'sha256': hashlib.sha256,
    'sha1': hashlib.sha1,
    'md5': hashlib.md5,
    'blake2b': hashlib.blake2b
}

# Logging configuration
LOG_FORMAT_CONSOLE = "%(asctime)s - %(levelname)-8s - [%(threadName)-12s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ============================================================================
# Global Events
# ============================================================================

shutdown_event = Event()
SENTINEL = object()  # Queue sentinel value

# ============================================================================
# Module Logger
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class Job:
    """Represents a file compression job."""
    source_path: Path
    temp_uncompressed: Optional[Path] = None
    temp_compressed: Optional[Path] = None
    dest_final: Optional[Path] = None
    size_bytes: int = 0
    compressed_size: int = 0
    source_checksum: Optional[str] = None
    compressed_checksum: Optional[str] = None
    start_time: float = field(default_factory=time.time)
    download_time: float = 0
    compress_time: float = 0
    upload_time: float = 0
    action: str = "pending"  # processed | skip_existing | skip_already_compressed | skip_due_to_race | failed

@dataclass
class PipelineStats:
    """Tracks pipeline statistics."""
    files_discovered: int = 0
    bytes_discovered: int = 0
    files_downloaded: int = 0
    files_compressed: int = 0
    files_uploaded: int = 0
    files_skipped_existing: int = 0
    files_skipped_already_compressed: int = 0
    files_skipped_race: int = 0
    files_with_both_versions: int = 0  # Files that have both .ext and .ext.gz
    files_failed: int = 0
    bytes_processed: int = 0
    bytes_compressed: int = 0
    total_download_time: float = 0
    total_compress_time: float = 0
    total_upload_time: float = 0

    # Pipeline efficiency metrics
    pipeline_idle_time: float = 0
    download_wait_time: float = 0  # Time downloader waited for queue space
    compress_wait_time: float = 0  # Time compressor waited for input
    upload_wait_time: float = 0    # Time uploader waited for input

    start_time: float = field(default_factory=time.time)
    lock: Lock = field(default_factory=Lock)

# ============================================================================
# Signal Handling
# ============================================================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    signal_name = signal.Signals(signum).name
    logger.warning(f"Received {signal_name} signal. Shutting down gracefully...")
    shutdown_event.set()

# ============================================================================
# Utility Functions
# ============================================================================

def format_bytes(size_bytes: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

def safe_unlink(path: Optional[Path]):
    """Safely remove a file if it exists."""
    if path and path.exists():
        try:
            path.unlink()
            logger.debug(f"Removed: {path}")
        except OSError as e:
            logger.warning(f"Failed to remove {path}: {e}")

def get_free_space(path: Path) -> int:
    """Get free space in bytes for the filesystem containing path."""
    stat = os.statvfs(path if path.is_dir() else path.parent)
    return stat.f_bavail * stat.f_frsize

def create_temp_path(source_path: Path, temp_dir: Path, suffix: str = "") -> Path:
    """Create a unique temp path based on source file."""
    # Use hash of absolute source path to avoid collisions
    abs_path = str(source_path.resolve())
    path_hash = hashlib.sha1(abs_path.encode()).hexdigest()[:8]
    basename = source_path.name
    temp_name = f"{basename}.{path_hash}{suffix}"
    return temp_dir / temp_name

# ============================================================================
# Checksum Functions
# ============================================================================

def calculate_checksum(file_path: Path, algo: str = 'sha256',
                       buffer_size: int = DEFAULT_BUFFER_SIZE) -> str:
    """Calculate checksum of a file."""
    hash_func = CHECKSUM_ALGORITHMS[algo]()

    with open(file_path, 'rb') as f:
        while chunk := f.read(buffer_size):
            hash_func.update(chunk)

    return hash_func.hexdigest()

def verify_copy(src_path: Path, dst_path: Path, algo: str = 'sha256',
                buffer_size: int = DEFAULT_BUFFER_SIZE,
                src_checksum: Optional[str] = None) -> str:
    """Verify that destination matches source and return source checksum."""
    if src_checksum is None:
        src_checksum = calculate_checksum(src_path, algo, buffer_size)

    dst_checksum = calculate_checksum(dst_path, algo, buffer_size)

    if src_checksum != dst_checksum:
        raise ValueError(f"Checksum mismatch: {src_checksum} != {dst_checksum}")

    return src_checksum

# ============================================================================
# File Operations with Retry
# ============================================================================

def copy_with_retry(src: Path, dst: Path, retries: int = DEFAULT_RETRIES,
                    retry_wait: float = DEFAULT_RETRY_WAIT,
                    buffer_size: int = DEFAULT_BUFFER_SIZE) -> None:
    """Copy file with retries and exponential backoff, with network timeout protection."""
    last_error = None
    wait_time = retry_wait

    for attempt in range(retries + 1):
        if shutdown_event.is_set():
            raise InterruptedError("Shutdown requested")

        try:
            # Ensure destination directory exists
            dst.parent.mkdir(parents=True, exist_ok=True)

            # Use dd with progress monitoring
            # status=progress shows transfer progress to stderr
            file_size = src.stat().st_size

            # Start dd process with progress output
            cmd = [
                'dd',
                f'if={src}',
                f'of={dst}',
                f'bs={buffer_size}',
                'status=progress'
            ]

            process = subprocess.Popen(
                cmd,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Monitor progress with timeout for stalls
            last_progress_time = time.time()
            last_bytes_copied = 0
            stall_timeout = 30  # Consider stalled if no progress for 30 seconds

            while True:
                # Check if process is still running
                poll = process.poll()
                if poll is not None:
                    # Process finished
                    if poll == 0:
                        return  # Success
                    else:
                        # Get any remaining error output
                        _, stderr = process.communicate(timeout=1)
                        raise IOError(f"Copy failed with code {poll}: {stderr}")

                # Check for output with timeout
                try:
                    # Use select to check if data is available
                    import select
                    readable, _, _ = select.select([process.stderr], [], [], 1.0)

                    if readable:
                        # Read available progress data
                        line = process.stderr.readline()
                        if line:
                            # dd progress format: "1234567890 bytes (1.2 GB, 1.1 GiB) copied, 5 s, 247 MB/s"
                            if 'bytes' in line and 'copied' in line:
                                try:
                                    bytes_copied = int(line.split()[0])
                                    if bytes_copied > last_bytes_copied:
                                        # Progress made, reset stall timer
                                        last_bytes_copied = bytes_copied
                                        last_progress_time = time.time()
                                except (ValueError, IndexError):
                                    pass  # Ignore parsing errors

                    # Check for stall
                    if time.time() - last_progress_time > stall_timeout:
                        # No progress for too long, kill the process
                        process.terminate()
                        process.wait(timeout=5)
                        raise TimeoutError(f"Copy stalled - no progress for {stall_timeout} seconds")

                    # Check for shutdown
                    if shutdown_event.is_set():
                        process.terminate()
                        process.wait(timeout=5)
                        raise InterruptedError("Shutdown requested")

                except subprocess.TimeoutExpired:
                    # No output available, continue monitoring
                    pass

        except Exception as e:
            last_error = e
            # Clean up partial file
            if dst.exists():
                try:
                    dst.unlink()
                except:
                    pass

            if attempt < retries:
                logger.warning(f"Copy failed (attempt {attempt + 1}/{retries + 1}): {e}")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
            else:
                break

    raise IOError(f"Failed to copy after {retries + 1} attempts: {last_error}")

def run_pigz(input_path: Path, output_path: Path, threads: int,
             pigz_path: str = 'pigz', retries: int = DEFAULT_RETRIES,
             retry_wait: float = DEFAULT_RETRY_WAIT) -> None:
    """Run pigz compression with retries."""
    last_error = None
    wait_time = retry_wait

    for attempt in range(retries + 1):
        if shutdown_event.is_set():
            raise InterruptedError("Shutdown requested")

        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Run pigz with specified threads
            cmd = [pigz_path, '-p', str(threads), '-c', str(input_path)]

            with open(output_path, 'wb') as fout:
                result = subprocess.run(
                    cmd,
                    stdout=fout,
                    stderr=subprocess.PIPE,
                    check=True
                )

            return

        except subprocess.CalledProcessError as e:
            last_error = f"pigz failed: {e.stderr.decode() if e.stderr else 'Unknown error'}"
            safe_unlink(output_path)  # Clean up partial file

        except Exception as e:
            last_error = str(e)
            safe_unlink(output_path)

        if attempt < retries:
            logger.warning(f"Compression failed (attempt {attempt + 1}/{retries + 1}): {last_error}")
            time.sleep(wait_time)
            wait_time *= 2

    raise RuntimeError(f"Compression failed after {retries + 1} attempts: {last_error}")

# ============================================================================
# File Discovery
# ============================================================================

def discover_files_from_directories(directories: List[Path], extensions: List[str]) -> Tuple[List[Path], dict]:
    """
    Recursively discover files in fastq_pass/fastq_fail directories from multiple source directories.
    """
    files = []
    extensions_set = set(extensions)

    logger.info(f"Discovering files in {len(directories)} directories")
    logger.info(f"Looking for extensions: {', '.join(extensions)}")
    logger.info(f"Searching in directories: {', '.join(FASTQ_DIRECTORY_NAMES)}")

    def should_include_file(path: Path) -> bool:
        """Check if file should be included in discovery."""
        # Include uncompressed files with target extensions
        if path.suffix in extensions_set:
            return True
        # Include compressed versions of target extensions
        for ext in extensions_set:
            if path.name.endswith(f'{ext}.gz'):
                return True
        return False

    # Track directory states
    dir_stats = {}

    # Walk each source directory tree
    for src_dir in directories:
        if not src_dir.exists():
            logger.warning(f"Directory not found: {src_dir}")
            continue

        dir_uncompressed = 0
        dir_compressed = 0
        dir_both = 0

        for root, dirs, filenames in os.walk(src_dir):
            root_path = Path(root)

            # Check if we're in or under a fastq_pass/fastq_fail directory
            current = root_path
            in_fastq_dir = False
            while current != current.parent:
                if current.name in FASTQ_DIRECTORY_NAMES:
                    in_fastq_dir = True
                    break
                current = current.parent

            if in_fastq_dir:
                # Track files by base name to find pairs
                base_files = {}

                for filename in filenames:
                    file_path = root_path / filename
                    if should_include_file(file_path):
                        # Determine base name (without .gz if present)
                        if str(file_path).endswith('.gz'):
                            base_name = str(file_path)[:-3]
                            is_compressed = True
                        else:
                            base_name = str(file_path)
                            is_compressed = False

                        if base_name not in base_files:
                            base_files[base_name] = {'uncompressed': None, 'compressed': None}

                        if is_compressed:
                            base_files[base_name]['compressed'] = file_path
                        else:
                            base_files[base_name]['uncompressed'] = file_path
                            files.append(file_path)  # Only add uncompressed files to process list

                # Count file states
                for base_name, versions in base_files.items():
                    if versions['uncompressed'] and versions['compressed']:
                        dir_both += 1
                    elif versions['compressed']:
                        dir_compressed += 1
                    elif versions['uncompressed']:
                        dir_uncompressed += 1

        # Determine directory state
        total = dir_uncompressed + dir_compressed + dir_both
        if total > 0:
            if dir_both == total:
                state = "ALL_BOTH"
            elif dir_compressed == total:
                state = "ALL_COMPRESSED"
            elif dir_uncompressed == total:
                state = "ALL_UNCOMPRESSED"
            else:
                state = "MIXED"

            dir_stats[str(src_dir)] = {
                'state': state,
                'uncompressed_only': dir_uncompressed,
                'compressed_only': dir_compressed,
                'both_versions': dir_both,
                'total': total
            }

            logger.info(f"Directory {src_dir.name}: {state} "
                       f"(uncompressed={dir_uncompressed}, compressed={dir_compressed}, both={dir_both})")

    # Sort for deterministic ordering
    files.sort()

    logger.info(f"Discovered {len(files)} files to process")

    return files, dir_stats

# ============================================================================
# Pipeline Workers
# ============================================================================

def downloader(
    files: List[Path],
    compress_queue: queue.Queue,
    temp_dir: Path,
    checksum_algo: str,
    stats: PipelineStats,
    retries: int,
    retry_wait: float
):
    """
    Stage 1: Download files sequentially to temp directory.
    """
    logger.info("Downloader started")

    for source_path in files:
        if shutdown_event.is_set():
            logger.info("Downloader shutting down")
            break

        # Skip .gz files early to avoid wasted I/O
        if source_path.suffix == '.gz':
            logger.info(f"[Download] Skipping already compressed: {source_path.name}")
            job = Job(
                source_path=source_path,
                action="skip_already_compressed"
            )
            compress_queue.put(job)
            continue

        temp_uncompressed = None  # Initialize for cleanup
        try:
            # Get file size
            size_bytes = source_path.stat().st_size

            # Check temp space (need 1.5x for compressed + uncompressed)
            required_space = int(size_bytes * 1.5)
            while get_free_space(temp_dir) < required_space:
                if shutdown_event.is_set():
                    break
                logger.warning(f"Waiting for temp space: need {format_bytes(required_space)}")
                time.sleep(5)

            if shutdown_event.is_set():
                break

            # Wait if queue is full (track wait time) - only if queue has a limit
            if compress_queue.maxsize > 0:
                wait_start = time.time()
                while compress_queue.qsize() >= compress_queue.maxsize:
                    if shutdown_event.is_set():
                        break
                    time.sleep(0.1)

                with stats.lock:
                    stats.download_wait_time += time.time() - wait_start

            if shutdown_event.is_set():
                break

            # Create temp path
            temp_uncompressed = create_temp_path(source_path, temp_dir)

            logger.info(f"[Download] {source_path.name} ({format_bytes(size_bytes)})")

            download_start = time.time()

            # Calculate source checksum first (before copy)
            logger.debug(f"[Download] Calculating source checksum")
            source_checksum = calculate_checksum(source_path, checksum_algo)

            # Copy to temp with retries
            copy_with_retry(source_path, temp_uncompressed, retries, retry_wait)

            # Verify copy using pre-calculated source checksum
            verify_copy(source_path, temp_uncompressed, checksum_algo,
                       src_checksum=source_checksum)

            download_time = time.time() - download_start

            # Create job
            job = Job(
                source_path=source_path,
                temp_uncompressed=temp_uncompressed,
                size_bytes=size_bytes,
                source_checksum=source_checksum,
                download_time=download_time
            )

            # Update stats
            with stats.lock:
                stats.files_downloaded += 1
                stats.total_download_time += download_time

            dt = max(download_time, 1e-9)
            throughput = size_bytes / dt / (1024**2)  # MB/s
            logger.info(f"[Download] Complete: {format_duration(download_time)}, {throughput:.1f} MB/s")

            # Enqueue job
            compress_queue.put(job)

        except Exception as e:
            logger.error(f"Download failed for {source_path}: {e}")
            with stats.lock:
                stats.files_failed += 1
            # Cleanup temp file if it was created
            if temp_uncompressed is not None:
                safe_unlink(temp_uncompressed)
            if not shutdown_event.is_set():
                shutdown_event.set()
            break

    # Send sentinel to indicate completion
    compress_queue.put(SENTINEL)
    logger.info("Downloader completed")

def compressor(
    compress_queue: queue.Queue,
    upload_queue: queue.Queue,
    temp_dir: Path,
    checksum_algo: str,
    pigz_path: str,
    pigz_threads: int,
    stats: PipelineStats,
    retries: int,
    retry_wait: float,
    fast_verify: bool
):
    """
    Stage 2: Compress files sequentially with pigz.
    """
    logger.info("Compressor started")

    while not shutdown_event.is_set():
        job = None  # Initialize for finally block

        # Track wait time
        wait_start = time.time()

        try:
            # Get next job with timeout to check shutdown
            try:
                job = compress_queue.get(timeout=1)
            except queue.Empty:
                with stats.lock:
                    stats.compress_wait_time += time.time() - wait_start
                continue

            if job is SENTINEL:
                logger.info("Compressor received sentinel")
                # Forward sentinel to upload queue
                upload_queue.put(SENTINEL)
                break

            with stats.lock:
                stats.compress_wait_time += time.time() - wait_start

            # Check if this is a skip_already_compressed job from downloader
            if job.action == "skip_already_compressed":
                with stats.lock:
                    stats.files_skipped_already_compressed += 1
                # Forward to uploader for consistent counting
                upload_queue.put(job)
                continue

            # In-place compression only
            job.dest_final = job.source_path.with_suffix(job.source_path.suffix + '.gz')

            # Check if both compressed and uncompressed versions exist
            uncompressed_exists = job.source_path.exists()
            compressed_exists = job.dest_final.exists()

            if uncompressed_exists and compressed_exists:
                # Log warning about both versions existing
                uncompressed_size = job.source_path.stat().st_size
                compressed_size = job.dest_final.stat().st_size
                logger.warning(f"[BOTH EXIST] {job.source_path.name} - "
                             f"uncompressed: {format_bytes(uncompressed_size)}, "
                             f"compressed: {format_bytes(compressed_size)}")
                with stats.lock:
                    stats.files_with_both_versions += 1

            # Early existence check to save CPU
            if compressed_exists:
                dest_size = job.dest_final.stat().st_size

                if not fast_verify:
                    # Do checksum verification of existing file
                    try:
                        existing_checksum = calculate_checksum(job.dest_final, checksum_algo)
                        logger.info(f"[Skip] {job.source_path.name} - exists "
                                   f"({format_bytes(dest_size)}, checksum: {existing_checksum[:8]}...)")
                    except Exception as e:
                        logger.warning(f"[Skip] {job.source_path.name} - exists "
                                      f"({format_bytes(dest_size)}, verify failed: {e})")
                else:
                    logger.info(f"[Skip] {job.source_path.name} - exists "
                               f"({format_bytes(dest_size)}, fast-verify)")

                job.action = "skip_existing"
                with stats.lock:
                    stats.files_skipped_existing += 1
                safe_unlink(job.temp_uncompressed)
                # Forward to uploader for cleanup tracking
                upload_queue.put(job)
                continue

            # Compress
            logger.info(f"[Compress] {job.source_path.name} with {pigz_threads} threads")
            compress_start = time.time()

            job.temp_compressed = create_temp_path(job.source_path, temp_dir, '.gz')

            run_pigz(
                job.temp_uncompressed,
                job.temp_compressed,
                pigz_threads,
                pigz_path,
                retries,
                retry_wait
            )

            job.compress_time = time.time() - compress_start
            with stats.lock:
                stats.total_compress_time += job.compress_time
                stats.files_compressed += 1

            job.compressed_size = job.temp_compressed.stat().st_size
            ratio = 0.0 if job.size_bytes == 0 else (1 - job.compressed_size / job.size_bytes) * 100
            logger.info(f"[Compress] Complete: {format_duration(job.compress_time)}, "
                       f"ratio: {ratio:.1f}%")

            # Calculate compressed checksum
            job.compressed_checksum = calculate_checksum(job.temp_compressed, checksum_algo)

            # Check again before queuing upload (race window minimization)
            if job.dest_final.exists():
                logger.info(f"[Skip] {job.source_path.name} - appeared during compression")
                job.action = "skip_due_to_race"
                with stats.lock:
                    stats.files_skipped_race += 1
                safe_unlink(job.temp_uncompressed)
                safe_unlink(job.temp_compressed)
                upload_queue.put(job)
                continue

            # Queue for upload
            upload_queue.put(job)

            # Can now cleanup uncompressed temp (compressed temp still needed)
            safe_unlink(job.temp_uncompressed)

        except Exception as e:
            logger.error(f"Compressor error: {e}", exc_info=True)
            with stats.lock:
                stats.files_failed += 1

            # Cleanup on failure
            if job is not None and job != SENTINEL:
                safe_unlink(job.temp_uncompressed)
                safe_unlink(job.temp_compressed)

            if not shutdown_event.is_set():
                shutdown_event.set()
            break

        finally:
            if job is not None and job != SENTINEL:
                compress_queue.task_done()

    logger.info("Compressor completed")

def uploader(
    upload_queue: queue.Queue,
    checksum_algo: str,
    stats: PipelineStats,
    retries: int,
    retry_wait: float
):
    """
    Stage 3: Upload compressed files sequentially to destination.
    """
    logger.info("Uploader started")

    while not shutdown_event.is_set():
        job = None  # Initialize for finally block

        # Track wait time
        wait_start = time.time()

        try:
            # Get next job with timeout
            try:
                job = upload_queue.get(timeout=1)
            except queue.Empty:
                with stats.lock:
                    stats.upload_wait_time += time.time() - wait_start
                continue

            if job is SENTINEL:
                logger.info("Uploader received sentinel")
                break

            with stats.lock:
                stats.upload_wait_time += time.time() - wait_start

            # Skip jobs that were already handled
            if job.action in ("skip_existing", "skip_already_compressed", "skip_due_to_race"):
                continue

            # Upload
            logger.info(f"[Upload] {job.source_path.name} to {job.dest_final}")
            upload_start = time.time()

            # Ensure destination directory exists
            job.dest_final.parent.mkdir(parents=True, exist_ok=True)

            # Upload to .part file
            dest_part = job.dest_final.with_suffix(job.dest_final.suffix + '.part')

            # Remove any stale .part file
            safe_unlink(dest_part)

            # Copy to .part
            copy_with_retry(job.temp_compressed, dest_part, retries, retry_wait)

            # Verify upload
            uploaded_checksum = calculate_checksum(dest_part, checksum_algo)
            if uploaded_checksum != job.compressed_checksum:
                safe_unlink(dest_part)
                raise ValueError(f"Upload verification failed")

            # Atomic no-clobber publish
            try:
                # Try hardlink first (fastest, most atomic)
                try:
                    os.link(dest_part, job.dest_final)
                    os.unlink(dest_part)
                except OSError as e:
                    # Check for filesystem limitations
                    unsupported_errnos = [errno.EPERM, errno.EACCES, errno.EXDEV]
                    if hasattr(errno, 'ENOTSUP'):
                        unsupported_errnos.append(errno.ENOTSUP)

                    if e.errno in unsupported_errnos:
                        # Filesystem doesn't support hardlinks, use fallback
                        logger.debug(f"Hardlink not supported, using O_EXCL fallback")

                        try:
                            fd = os.open(str(job.dest_final),
                                       os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                                       0o644)
                            try:
                                with open(dest_part, 'rb') as src:
                                    while chunk := src.read(DEFAULT_BUFFER_SIZE):
                                        os.write(fd, chunk)
                            finally:
                                os.close(fd)

                            # Verify final matches expected checksum
                            final_checksum = calculate_checksum(job.dest_final, checksum_algo)
                            if final_checksum != job.compressed_checksum:
                                safe_unlink(job.dest_final)
                                safe_unlink(dest_part)
                                raise ValueError(f"Publish verify failed in O_EXCL path: "
                                               f"{final_checksum} != {job.compressed_checksum}")

                            # Success - remove .part
                            os.unlink(dest_part)
                        except FileExistsError:
                            safe_unlink(dest_part)
                            raise
                        except Exception:
                            # Clean up both .part and potentially corrupt final
                            safe_unlink(dest_part)
                            safe_unlink(job.dest_final)
                            raise
                    else:
                        safe_unlink(dest_part)
                        raise

                job.upload_time = time.time() - upload_start
                with stats.lock:
                    stats.total_upload_time += job.upload_time
                    stats.files_uploaded += 1
                    stats.bytes_processed += job.size_bytes
                    stats.bytes_compressed += job.compressed_size

                ut = max(job.upload_time, 1e-9)
                throughput = job.compressed_size / ut / (1024**2)
                logger.info(f"[Upload] Complete: {format_duration(job.upload_time)}, "
                           f"{throughput:.1f} MB/s")

                # Success - cleanup compressed temp
                job.action = "processed"
                safe_unlink(job.temp_compressed)

            except (FileExistsError, OSError) as e:
                if isinstance(e, FileExistsError) or (isinstance(e, OSError) and e.errno == errno.EEXIST):
                    # Race condition - destination appeared
                    safe_unlink(dest_part)
                    logger.info(f"[Skip] {job.source_path.name} - appeared during upload")
                    job.action = "skip_due_to_race"
                    with stats.lock:
                        stats.files_skipped_race += 1
                    safe_unlink(job.temp_compressed)
                    continue
                else:
                    raise

        except Exception as e:
            logger.error(f"Uploader error: {e}", exc_info=True)
            with stats.lock:
                stats.files_failed += 1

            # Cleanup on failure
            if job is not None and job != SENTINEL:
                safe_unlink(job.temp_compressed)

            if not shutdown_event.is_set():
                shutdown_event.set()
            break

        finally:
            if job is not None and job != SENTINEL:
                upload_queue.task_done()

    logger.info("Uploader completed")

# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging(log_file: Optional[Path] = None, log_level: str = "INFO"):
    """Configure logging with console and optional file output."""
    # Set base logger level
    logger.setLevel(getattr(logging, log_level.upper()))

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT_CONSOLE, LOG_DATE_FORMAT))
    console_handler.setLevel(getattr(logging, log_level.upper()))
    logger.addHandler(console_handler)

    # File handler if requested
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(LOG_FORMAT_CONSOLE, LOG_DATE_FORMAT))
            file_handler.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)
            logger.debug(f"Logging to file: {log_file}")
        except (IOError, PermissionError) as e:
            logger.error(f"Failed to create log file {log_file}: {e}")

# ============================================================================
# Main Pipeline
# ============================================================================

class CompressionPipeline:
    """Main pipeline orchestrator."""

    def __init__(
        self,
        directories: List[Path],
        file_paths: List[Path],
        temp_dir: Path,
        file_extensions: List[str],
        is_file_list_mode: bool,
        checksum_algo: str,
        pigz_path: str,
        pigz_threads: int,
        compress_queue_size: int,
        upload_queue_size: int,
        retries: int,
        retry_wait: float,
        fast_verify: bool
    ):
        self.directories = directories
        self.file_paths = file_paths
        self.temp_dir = temp_dir
        self.file_extensions = file_extensions
        self.is_file_list_mode = is_file_list_mode
        self.checksum_algo = checksum_algo
        self.pigz_path = pigz_path
        self.pigz_threads = pigz_threads
        self.compress_queue_size = compress_queue_size
        self.upload_queue_size = upload_queue_size
        self.retries = retries
        self.retry_wait = retry_wait
        self.fast_verify = fast_verify

        # Create temp directory
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Clean up stale .part files in temp
        self._cleanup_temp_parts()

        # Statistics
        self.stats = PipelineStats()

    def _cleanup_temp_parts(self):
        """Remove stale .part files from temp directory."""
        for part_file in self.temp_dir.glob("*.part"):
            logger.info(f"Removing stale temp file: {part_file}")
            safe_unlink(part_file)

    def validate_paths(self):
        """Validate that temp is safe and directories/files exist."""
        temp_abs = self.temp_dir.resolve()

        if self.is_file_list_mode:
            # Validate temp not conflicting with file paths
            for file_path in self.file_paths:
                if temp_abs == file_path.parent.resolve():
                    logger.warning(f"Temp directory same as file parent: {file_path.parent}")
        else:
            # Check temp not inside any source directory
            for directory in self.directories:
                dir_abs = directory.resolve()
                if temp_abs == dir_abs:
                    raise ValueError(f"Temp directory cannot be same as source: {directory}")
                if temp_abs.is_relative_to(dir_abs):
                    raise ValueError(f"Temp cannot be inside source directory: {directory}")

            logger.info(f"Processing {len(self.directories)} directories")

        logger.info(f"Temp directory: {temp_abs}")

    def run(self):
        """Run the compression pipeline."""
        self.validate_paths()

        # Get files based on mode
        dir_stats = {}
        if self.is_file_list_mode:
            # Use provided file list, filter out already compressed
            files = []
            for path in self.file_paths:
                if not str(path).endswith('.gz'):
                    files.append(path)
                else:
                    logger.debug(f"Skipping already compressed: {path}")
        else:
            # Discover files from directories
            files, dir_stats = discover_files_from_directories(self.directories, self.file_extensions)

            # Print directory state summary
            if dir_stats:
                print("\n" + "=" * 80)
                print("DIRECTORY COMPRESSION STATE ANALYSIS")
                print("=" * 80)
                for dir_path, stats in dir_stats.items():
                    dir_name = Path(dir_path).name
                    print(f"\n{dir_name}: {stats['state']}")
                    print(f"  - Uncompressed only: {stats['uncompressed_only']}")
                    print(f"  - Compressed only: {stats['compressed_only']}")
                    print(f"  - Both versions exist: {stats['both_versions']}")
                    print(f"  - Total files: {stats['total']}")
                print("=" * 80 + "\n")

        if not files:
            logger.warning("No files found to process")
            return

        # Calculate total size
        total_bytes = sum(f.stat().st_size for f in files)
        self.stats.files_discovered = len(files)
        self.stats.bytes_discovered = total_bytes

        logger.info(f"Total files queued: {len(files)}")
        logger.info(f"Total size: {format_bytes(total_bytes)}")

        # Create queues (0 or negative means unlimited)
        compress_queue = queue.Queue(maxsize=self.compress_queue_size if self.compress_queue_size > 0 else 0)
        upload_queue = queue.Queue(maxsize=self.upload_queue_size if self.upload_queue_size > 0 else 0)

        queue_compress_desc = "unlimited" if self.compress_queue_size <= 0 else str(self.compress_queue_size)
        queue_upload_desc = "unlimited" if self.upload_queue_size <= 0 else str(self.upload_queue_size)
        logger.info(f"Queue sizes: compress={queue_compress_desc}, upload={queue_upload_desc}")

        # Check available temp space
        temp_stats = shutil.disk_usage(self.temp_dir)
        available_gb = temp_stats.free / (1024**3)

        # Calculate required space based on queue configuration
        if self.compress_queue_size <= 0 and self.upload_queue_size <= 0:
            # Both queues unlimited - could need space for all files
            required_bytes = total_bytes
            logger.warning("Both queues are unlimited - may require full dataset space in temp")
        elif self.compress_queue_size <= 0:
            # Compress queue unlimited, upload queue limited
            # Worst case: all files downloaded, upload_queue_size compressed
            max_compressed = min(self.upload_queue_size, len(files)) if self.upload_queue_size > 0 else len(files)
            required_bytes = total_bytes
            logger.info(f"Compress queue unlimited - may buffer entire dataset")
        elif self.upload_queue_size <= 0:
            # Upload queue unlimited, compress queue limited
            # Worst case: compress_queue uncompressed + all compressed files
            max_uncompressed = min(self.compress_queue_size, len(files))
            avg_file_size = total_bytes / len(files) if files else 0
            # Assume compression ratio of ~4:1 for fastq files
            required_bytes = (max_uncompressed * avg_file_size) + (total_bytes * 0.25)
            logger.info(f"Upload queue unlimited - may buffer all compressed files")
        else:
            # Both queues limited
            max_uncompressed = min(self.compress_queue_size, len(files))
            max_compressed = min(self.upload_queue_size, len(files))
            avg_file_size = total_bytes / len(files) if files else 0
            # Space for uncompressed files in compress queue + compressed files in upload queue
            required_bytes = (max_uncompressed * avg_file_size) + (max_compressed * avg_file_size * 0.25)
            logger.info(f"Bounded queues - max temp usage controlled")

        required_gb = required_bytes / (1024**3)

        # Add 10% safety margin
        required_gb *= 1.1

        logger.info(f"Temp directory: {self.temp_dir}")
        logger.info(f"Available space: {available_gb:.2f} GB")
        logger.info(f"Estimated required: {required_gb:.2f} GB")

        if available_gb < required_gb:
            logger.error(f"Insufficient temp space! Need {required_gb:.2f} GB but only {available_gb:.2f} GB available")
            logger.error(f"Consider using bounded queues (--compress-queue and --upload-queue) to limit temp usage")
            logger.error(f"Or use a different temp directory with more space (--temp-dir)")
            sys.exit(1)

        if available_gb < required_gb * 2:
            logger.warning(f"Temp space is tight - only {available_gb/required_gb:.1f}x required space available")

        # Start threads
        downloader_thread = Thread(
            target=downloader,
            args=(
                files,
                compress_queue,
                self.temp_dir,
                self.checksum_algo,
                self.stats,
                self.retries,
                self.retry_wait
            ),
            name="Downloader"
        )

        compressor_thread = Thread(
            target=compressor,
            args=(
                compress_queue,
                upload_queue,
                self.temp_dir,
                self.checksum_algo,
                self.pigz_path,
                self.pigz_threads,
                self.stats,
                self.retries,
                self.retry_wait,
                self.fast_verify
            ),
            name="Compressor"
        )

        uploader_thread = Thread(
            target=uploader,
            args=(
                upload_queue,
                self.checksum_algo,
                self.stats,
                self.retries,
                self.retry_wait
            ),
            name="Uploader"
        )

        # Start all threads
        downloader_thread.start()
        compressor_thread.start()
        uploader_thread.start()

        # Wait for completion
        try:
            downloader_thread.join()
            compressor_thread.join()
            uploader_thread.join()
        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
            shutdown_event.set()
            downloader_thread.join(timeout=5)
            compressor_thread.join(timeout=5)
            uploader_thread.join(timeout=5)

        # Print summary
        self.print_summary()

        # Cleanup temp directory if empty
        try:
            if not any(self.temp_dir.iterdir()):
                self.temp_dir.rmdir()
        except OSError:
            pass

        # Exit with appropriate code
        if self.stats.files_failed > 0 or shutdown_event.is_set():
            sys.exit(1)

    def print_summary(self):
        """Print final summary statistics."""
        duration = time.time() - self.stats.start_time

        print("\n" + "=" * 80)
        print("COMPRESSION PIPELINE SUMMARY")
        print("=" * 80)

        print(f"Total runtime: {format_duration(duration)}")
        print(f"Files discovered: {self.stats.files_discovered}")
        print(f"Files processed: {self.stats.files_uploaded}")
        print(f"Files skipped (existing): {self.stats.files_skipped_existing}")
        print(f"Files skipped (already .gz): {self.stats.files_skipped_already_compressed}")
        print(f"Files skipped (race): {self.stats.files_skipped_race}")
        print(f"Files with both versions: {self.stats.files_with_both_versions}")
        print(f"Files failed: {self.stats.files_failed}")

        if self.stats.bytes_processed > 0:
            print(f"\nData processed: {format_bytes(self.stats.bytes_processed)}")
            print(f"Data compressed: {format_bytes(self.stats.bytes_compressed)}")
            compression_ratio = (1 - self.stats.bytes_compressed / self.stats.bytes_processed) * 100
            print(f"Overall compression: {compression_ratio:.1f}%")
            throughput = self.stats.bytes_processed / duration / (1024**2)
            print(f"Overall throughput: {throughput:.1f} MB/s")

        # Stage timing
        if self.stats.files_downloaded > 0:
            avg_download = self.stats.total_download_time / self.stats.files_downloaded
            print(f"\nAverage download time: {format_duration(avg_download)}")

        if self.stats.files_compressed > 0:
            avg_compress = self.stats.total_compress_time / self.stats.files_compressed
            print(f"Average compress time: {format_duration(avg_compress)}")

        if self.stats.files_uploaded > 0:
            avg_upload = self.stats.total_upload_time / self.stats.files_uploaded
            print(f"Average upload time: {format_duration(avg_upload)}")

        # Pipeline efficiency
        print(f"\nPipeline Efficiency:")
        print(f"  Download wait time: {format_duration(self.stats.download_wait_time)}")
        print(f"  Compress wait time: {format_duration(self.stats.compress_wait_time)}")
        print(f"  Upload wait time: {format_duration(self.stats.upload_wait_time)}")

        total_wait = (self.stats.download_wait_time +
                     self.stats.compress_wait_time +
                     self.stats.upload_wait_time)
        efficiency = max(0, (1 - total_wait / duration)) * 100
        print(f"  Pipeline efficiency: {efficiency:.1f}%")

        print("=" * 80)

# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Three-stage decoupled pipeline for file compression (optimized for slow networks)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
BEHAVIOR:
  In-place compression - compressed files land exactly where they were found.

Pipeline Architecture:
  Stage 1: Downloader - Sequential downloads to temp
  Stage 2: Compressor - Sequential compression with pigz
  Stage 3: Uploader   - Sequential uploads to destination

  Decoupled design allows compression to continue while uploading,
  maximizing throughput on slow networks.

Concurrency model:
  - Exactly one file downloading at any time
  - Exactly one pigz process at any time
  - Exactly one file uploading at any time
  - Compression and upload can overlap (different files)

Discovery:
  - Recursively searches for files in fastq_pass and fastq_fail directories
  - Default extensions: .fastq, .fq
  - Analyzes compression state of each directory
  - Reports: ALL_COMPRESSED, ALL_UNCOMPRESSED, ALL_BOTH, or MIXED

Safety:
  - Never overwrites existing destination files
  - Atomic uploads with race condition handling
  - Checksum verification at each transfer boundary
  - Clean temp file management

Example usage:
    # Directory mode: Process .fastq files in listed directories
    python3 compress_fastq_network.py directories.txt

    # Directory mode: Compress specific file types
    python3 compress_fastq_network.py directories.txt --file-type txt bed vcf tsv

    # File list mode: Process specific files listed in text file
    python3 compress_fastq_network.py --file-list files.txt

    # Tune for slow network (larger upload queue)
    python3 compress_fastq_network.py directories.txt \\
        --compress-queue 6 \\
        --upload-queue 3 \\
        --pigz-threads 8
"""
    )

    # Input arguments - either positional or --file-list
    parser.add_argument(
        'input_file',
        type=Path,
        nargs='?',
        help='Text file containing list of directories to process (one per line)'
    )

    parser.add_argument(
        '--file-list',
        type=Path,
        help='Text file containing list of files to compress (one per line)'
    )

    # File discovery options
    parser.add_argument(
        '--file-type',
        nargs='+',
        help='File extensions to compress (e.g., txt bed vcf tsv). '
             'Default: fastq. Extensions should be specified without the leading dot.'
    )


    # Processing options
    parser.add_argument(
        '--temp-dir',
        type=Path,
        default=Path('/tmp/fastq_compress'),
        help='Temporary directory for processing (default: /tmp/fastq_compress)'
    )

    parser.add_argument(
        '--pigz-path',
        default='pigz',
        help='Path to pigz executable (default: pigz)'
    )

    parser.add_argument(
        '--pigz-threads',
        type=str,
        default='auto',
        help='Number of pigz threads or "auto" for all CPUs (default: auto)'
    )

    parser.add_argument(
        '--checksum',
        choices=list(CHECKSUM_ALGORITHMS.keys()),
        default=DEFAULT_CHECKSUM_ALGO,
        help=f'Checksum algorithm (default: {DEFAULT_CHECKSUM_ALGO})'
    )

    # Queue configuration
    parser.add_argument(
        '--compress-queue',
        type=int,
        default=DEFAULT_COMPRESS_QUEUE_SIZE,
        help=f'Max files in compress queue (default: unlimited, use positive number to limit)'
    )

    parser.add_argument(
        '--upload-queue',
        type=int,
        default=DEFAULT_UPLOAD_QUEUE_SIZE,
        help=f'Max files in upload queue (default: unlimited, use positive number to limit)'
    )

    parser.add_argument(
        '--retries',
        type=int,
        default=DEFAULT_RETRIES,
        help=f'Number of retries for operations (default: {DEFAULT_RETRIES})'
    )

    parser.add_argument(
        '--retry-wait',
        type=float,
        default=DEFAULT_RETRY_WAIT,
        help=f'Initial wait between retries in seconds (default: {DEFAULT_RETRY_WAIT})'
    )

    # Logging options
    parser.add_argument(
        '--log-file',
        type=Path,
        help='Log file path'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )

    # Verification options
    parser.add_argument(
        '--fast-verify',
        action='store_true',
        help='Skip checksum calculation for existing destination files'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without executing'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.log_level)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Validate input arguments
    if not args.input_file and not args.file_list:
        parser.error("Either provide input_file (for directories) or use --file-list (for files)")

    if args.input_file and args.file_list:
        parser.error("Cannot use both positional input_file and --file-list. Choose one.")

    # Determine input mode
    is_file_list_mode = bool(args.file_list)
    input_path = args.file_list if is_file_list_mode else args.input_file

    # Log the mode being used
    mode_desc = "file list" if is_file_list_mode else "directory list"
    logger.info(f"Running in {mode_desc} mode")

    # Read input file
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    paths = []
    directories = []
    try:
        with input_path.open() as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    path = Path(line)
                    if is_file_list_mode:
                        # File list mode - add files directly
                        if path.exists() and path.is_file():
                            paths.append(path)
                        else:
                            logger.warning(f"Skipping invalid file: {line}")
                    else:
                        # Directory mode - collect directories
                        if path.exists() and path.is_dir():
                            directories.append(path)
                        else:
                            logger.warning(f"Skipping invalid directory: {line}")
    except IOError as e:
        logger.error(f"Failed to read input file: {e}")
        sys.exit(1)

    if not is_file_list_mode and not directories:
        logger.error("No valid directories found in input file")
        sys.exit(1)

    if is_file_list_mode and not paths:
        logger.error("No valid files found in input file")
        sys.exit(1)

    # Check pigz availability
    if not shutil.which(args.pigz_path):
        logger.error(f"pigz not found: {args.pigz_path}")
        logger.error("Install with: apt-get install pigz (Ubuntu) or brew install pigz (macOS)")
        sys.exit(1)

    # Determine file extensions to process
    file_extensions = []
    if args.file_type:
        # Convert user-provided extensions to proper format (add leading dot)
        for ext in args.file_type:
            ext = ext.strip()
            if not ext.startswith('.'):
                ext = '.' + ext
            file_extensions.append(ext)
        logger.info(f"Looking for file types: {', '.join(file_extensions)}")
    else:
        # Use default extensions
        file_extensions = DEFAULT_FILE_EXTENSIONS
        logger.info(f"Using default file types: {', '.join(file_extensions)}")

    # Determine pigz threads
    if args.pigz_threads == 'auto':
        pigz_threads = os.cpu_count() or 1
        logger.info(f"Using all {pigz_threads} CPU cores for pigz")
    else:
        try:
            pigz_threads = int(args.pigz_threads)
        except ValueError:
            logger.error(f"Invalid pigz-threads value: {args.pigz_threads}")
            sys.exit(1)

    # Handle dry-run mode
    if args.dry_run:
        logger.info("DRY-RUN MODE - showing what would be done")
        if is_file_list_mode:
            # Use files directly from list
            files = [p for p in paths if not str(p).endswith('.gz')]
        else:
            # Discover files from directories
            files, dir_stats = discover_files_from_directories(directories, file_extensions)

        if files:
            total_size = sum(f.stat().st_size for f in files)
            print(f"\nWould process {len(files)} files ({format_bytes(total_size)})")
            for f in files[:10]:  # Show first 10
                print(f"  - {f}")
            if len(files) > 10:
                print(f"  ... and {len(files) - 10} more")
        else:
            print("No files found to process")
        sys.exit(0)

    # Create and run pipeline
    pipeline = CompressionPipeline(
        directories=directories if not is_file_list_mode else [],
        file_paths=paths if is_file_list_mode else [],
        temp_dir=args.temp_dir,
        file_extensions=file_extensions,
        is_file_list_mode=is_file_list_mode,
        checksum_algo=args.checksum,
        pigz_path=args.pigz_path,
        pigz_threads=pigz_threads,
        compress_queue_size=args.compress_queue,
        upload_queue_size=args.upload_queue,
        retries=args.retries,
        retry_wait=args.retry_wait,
        fast_verify=args.fast_verify
    )

    try:
        pipeline.run()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()