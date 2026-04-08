"""Execute frpt commands and capture output."""

import re
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Callable
import logging

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import Settings
from src.cache import FrptCache

logger = logging.getLogger(__name__)


# Validation pattern for safe parameter values (allows ~, %, alphanumeric, underscore, hyphen)
SAFE_PARAM_PATTERN = re.compile(r"^[A-Za-z0-9_\-~%]+$")


def validate_param(value: str, param_name: str) -> str:
    """Validate parameter value to prevent injection.

    Args:
        value: Parameter value to validate
        param_name: Name of parameter for error messages

    Returns:
        Validated value

    Raises:
        ValueError: If value contains unsafe characters
    """
    if not SAFE_PARAM_PATTERN.match(value):
        raise ValueError(f"Invalid {param_name}: contains unsafe characters")
    return value


@dataclass(frozen=True)
class FrptCommand:
    """Immutable frpt command configuration."""

    step: str
    form_factor: str
    workweek: str
    dbase: str = Settings.DEFAULT_DESIGN_ID
    facility: str = Settings.DEFAULT_FACILITY

    def __post_init__(self) -> None:
        """Validate all parameters on initialization."""
        validate_param(self.step, "step")
        validate_param(self.form_factor, "form_factor")
        validate_param(self.workweek, "workweek")
        validate_param(self.dbase, "dbase")
        validate_param(self.facility, "facility")

    def build(self) -> str:
        """Build the frpt command string based on step type."""
        template = Settings.get_command_template(self.step)
        return template.format(
            dbase=self.dbase,
            step=self.step,
            form_factor=self.form_factor,
            workweek=self.workweek,
            facility=self.facility,
        )

    def build_args(self) -> list[str]:
        """Build the frpt command as argument list for shell=False execution."""
        cmd_str = self.build()
        return shlex.split(cmd_str)


@dataclass(frozen=True)
class FrptResult:
    """Immutable result from frpt command execution."""

    command: str
    stdout: str
    stderr: str
    return_code: int
    success: bool

    @classmethod
    def from_completed_process(
        cls, command: str, process: subprocess.CompletedProcess
    ) -> "FrptResult":
        """Create FrptResult from subprocess result."""
        return cls(
            command=command,
            stdout=process.stdout,
            stderr=process.stderr,
            return_code=process.returncode,
            success=process.returncode == 0,
        )

    @classmethod
    def from_error(cls, command: str, error: str) -> "FrptResult":
        """Create FrptResult from an error condition."""
        return cls(
            command=command,
            stdout="",
            stderr=error,
            return_code=-1,
            success=False,
        )


class FrptRunner:
    """Execute frpt commands."""

    DEFAULT_TIMEOUT_SECONDS = 600  # 10 minutes - frpt can take 60+ seconds per query
    DEFAULT_MAX_WORKERS = 8  # Number of parallel workers

    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_workers: int = DEFAULT_MAX_WORKERS,
        cache: Optional[FrptCache] = None,
        use_cache: bool = True,
    ):
        """Initialize runner with timeout, parallel workers, and cache.

        Args:
            timeout: Command timeout in seconds (default 10 minutes)
            max_workers: Maximum parallel workers (default 8)
            cache: FrptCache instance (creates default if None and use_cache=True)
            use_cache: Whether to use caching (default True)
        """
        self._timeout = timeout
        self._max_workers = max_workers
        self._use_cache = use_cache
        self._cache = cache if cache else (FrptCache() if use_cache else None)

    def run(self, command: FrptCommand, skip_cache: bool = False) -> FrptResult:
        """Execute a single frpt command.

        Args:
            command: FrptCommand configuration
            skip_cache: If True, bypass cache and always run command

        Returns:
            FrptResult with command output
        """
        cmd_str = command.build()

        # Check cache first
        if self._use_cache and self._cache and not skip_cache:
            cached = self._cache.get(
                step=command.step,
                form_factor=command.form_factor,
                workweek=command.workweek,
                dbase=command.dbase,
                facility=command.facility,
            )
            if cached:
                logger.info(f"Using cached result for {command.step}/{command.form_factor}/WW{command.workweek}")
                return FrptResult(
                    command=cmd_str,
                    stdout=cached.stdout,
                    stderr=cached.stderr,
                    return_code=cached.return_code,
                    success=cached.success,
                )

        # Run the actual command
        try:
            # Use shell=True because frpt command relies on shell features
            # Input validation is done in FrptCommand.__post_init__
            process = subprocess.run(
                cmd_str,
                shell=True,
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
            result = FrptResult.from_completed_process(cmd_str, process)

            # Store successful results in cache
            if self._use_cache and self._cache and result.success:
                self._cache.set(
                    step=command.step,
                    form_factor=command.form_factor,
                    workweek=command.workweek,
                    dbase=command.dbase,
                    facility=command.facility,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    return_code=result.return_code,
                    success=result.success,
                )

            return result
        except subprocess.TimeoutExpired:
            return FrptResult.from_error(cmd_str, f"Command timed out after {self._timeout}s")
        except Exception as e:
            return FrptResult.from_error(cmd_str, str(e))

    def get_cache_stats(self) -> Optional[dict]:
        """Get cache statistics.

        Returns:
            Cache stats dictionary or None if caching disabled
        """
        if self._cache:
            return self._cache.get_stats()
        return None

    def clear_cache(self) -> int:
        """Clear all cached results.

        Returns:
            Number of entries cleared
        """
        if self._cache:
            return self._cache.clear()
        return 0

    def run_batch(
        self,
        steps: list[str],
        form_factors: list[str],
        workweeks: list[str],
        dbase: str = Settings.DEFAULT_DESIGN_ID,
        facility: str = Settings.DEFAULT_FACILITY,
    ) -> list[FrptResult]:
        """Execute multiple frpt commands for all combinations (sequential).

        Args:
            steps: List of test steps
            form_factors: List of module form factors
            workweeks: List of workweeks
            dbase: Database name
            facility: Test facility

        Returns:
            List of FrptResult objects
        """
        results = []
        for step in steps:
            for form_factor in form_factors:
                for workweek in workweeks:
                    try:
                        command = FrptCommand(
                            step=step,
                            form_factor=form_factor,
                            workweek=workweek,
                            dbase=dbase,
                            facility=facility,
                        )
                        result = self.run(command)
                        results.append(result)
                    except ValueError as e:
                        results.append(FrptResult.from_error(
                            f"frpt -step={step} -form_factor={form_factor}",
                            str(e)
                        ))
        return results

    def run_parallel(
        self,
        commands: list[FrptCommand],
        progress_callback: Optional[Callable[[int, int, FrptCommand], None]] = None,
    ) -> list[tuple[FrptCommand, FrptResult]]:
        """Execute multiple frpt commands in parallel.

        Args:
            commands: List of FrptCommand objects to execute
            progress_callback: Optional callback(completed, total, command) for progress updates

        Returns:
            List of (command, result) tuples in completion order
        """
        results = []
        total = len(commands)

        if total == 0:
            return results

        logger.info(f"Running {total} commands in parallel with {self._max_workers} workers")

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all commands
            future_to_command = {
                executor.submit(self.run, cmd): cmd for cmd in commands
            }

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_command):
                command = future_to_command[future]
                completed += 1

                try:
                    result = future.result()
                except Exception as e:
                    logger.error(f"Command failed with exception: {e}")
                    result = FrptResult.from_error(command.build(), str(e))

                results.append((command, result))

                if progress_callback:
                    progress_callback(completed, total, command)

                logger.info(
                    f"[{completed}/{total}] Completed: {command.step}/{command.form_factor}/WW{command.workweek} "
                    f"- success={result.success}"
                )

        return results
