import json
import subprocess
import typing
from pathlib import Path
from typing import List, Optional, Dict

import click
import logging

from cases import BenchmarkCase
from config import BenchmarkConfig, BenchmarkTypeBase


logger = logging.getLogger()
format_string = logging.Formatter("%(asctime)s  [%(levelname)-5.5s]  %(message)s")
console_handler = logging.StreamHandler()
console_handler.setFormatter(format_string)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


BenchmarkResult = Dict[str, any]


class BenchmarkRunner(object):
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.result_context = {}    # type: Dict[str, any]
        self.results = []           # type: List[BenchmarkResult]

    @staticmethod
    def _subprocess_run(args: List[str],
                        env: Optional[typing.Dict[str, str]] = None
                        ) -> str:
        if env is None:
            env = {}
        process_debug_string = ' '.join(args)
        if env:
            env_debug_string = ' '.join(f"{k}=\"{v}\"" for k,v in env.items())
            process_debug_string = env_debug_string + " " + process_debug_string
        logger.info(f"Executing {process_debug_string}")
        process = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        if process.returncode:
            logger.error(f"Failed with return code {process.returncode}, stderr output:")
            logger.error(process.stdout.decode("utf-8"))
            raise RuntimeError(f"Failed running {process_debug_string}")
        return process.stdout.decode("utf-8")

    def _compile(self, compiler: str) -> Path:
        # self._subprocess_run(["make", "clean"])
        command_line_args = []
        env = {}
        if compiler:
            env['CC'] = compiler
        if self.config.cflags:
            env['CFLAGS'] = ' '.join(self.config.cflags)
            self.result_context['cflags'] = env['CFLAGS']
        command_line_args += ["make", "-j10", "zstd"]
        self._subprocess_run(command_line_args, env=env)
        return Path("programs/zstd")

    def _get_run_args(self, executable: Path, args: List[str]):
        run_args = []
        if self.config.cpu is not None:
            run_args += ["taskset", "-c", str(self.config.cpu)]
        run_args += [str(executable)]
        run_args += ['-qb', f'-i{self.config.evaluation_time}']
        if self.config.realtime:
            run_args += ["--priority=rt"]
        run_args += args
        return run_args

    def create_result(self, case: BenchmarkCase, output: str) -> BenchmarkResult:
        result = self.result_context.copy()
        result.update(case.results_columns)
        result.update(case.process_output(output))
        return result

    def run_benchmark(self, executable: Path, benchmark: BenchmarkTypeBase):
        for case in benchmark.get_cases():
            run_args = self._get_run_args(executable, case.command_line_args)
            output = self._subprocess_run(run_args)
            self.results += [self.create_result(case, output)]

    def run(self):
        for compiler in self.config.compilers:
            self.result_context = {}
            if compiler:
                self.result_context['compiler'] = compiler
            executable = self._compile(compiler)
            for benchmark in self.config.benchmarks:
                self.run_benchmark(executable, benchmark)

    def save_results(self, out: typing.IO):
        out.writelines([json.dumps(result) for result in self.results])


@click.command()
@click.option('-c', '--config', default="config.json", help='Benchmark configuration file',
              type=click.Path(exists=True))
@click.option('-o', '--out', default="bench_results.json", help='Benchmark results file',
              type=click.File('wb'))
def bench(config: click.Path, out: click.File):
    logger.info("Starting benchmark")
    config = BenchmarkConfig.parse_file(config)
    logger.debug(f"Parsed config {config}")
    runner = BenchmarkRunner(config)
    runner.run()
    logger.info(f"Benchmark done")


if __name__ == '__main__':
    bench()
