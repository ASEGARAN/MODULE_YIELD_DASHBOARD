"""Execute frpt commands and capture output."""

import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import Settings


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

    DEFAULT_TIMEOUT_SECONDS = 300

    def __init__(self, timeout: int = DEFAULT_TIMEOUT_SECONDS):
        """Initialize runner with timeout.

        Args:
            timeout: Command timeout in seconds (default 5 minutes)
        """
        self._timeout = timeout

    def run(self, command: FrptCommand) -> FrptResult:
        """Execute a single frpt command.

        Args:
            command: FrptCommand configuration

        Returns:
            FrptResult with command output
        """
        cmd_str = command.build()
        cmd_args = command.build_args()
        try:
            process = subprocess.run(
                cmd_args,
                shell=False,
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
            return FrptResult.from_completed_process(cmd_str, process)
        except subprocess.TimeoutExpired:
            return FrptResult.from_error(cmd_str, f"Command timed out after {self._timeout}s")
        except Exception as e:
            return FrptResult.from_error(cmd_str, str(e))

    def run_batch(
        self,
        steps: list[str],
        form_factors: list[str],
        workweeks: list[str],
        dbase: str = Settings.DEFAULT_DESIGN_ID,
        facility: str = Settings.DEFAULT_FACILITY,
    ) -> list[FrptResult]:
        """Execute multiple frpt commands for all combinations.

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
