from typing import List, Optional, Literal, Union
from pydantic import BaseModel

from cases import BenchmarkCase, DecompressBenchmarkCase, CompressDecompressBenchmarkCase


class BenchmarkTypeBase(BaseModel):
    type: str
    repeats: int = 1

    def get_cases(self) -> List[BenchmarkCase]:
        raise NotImplementedError()


class BenchmarkDecompressFile(BenchmarkTypeBase):
    type: Literal['decompress-files']
    inputs: List[str]
    additional_flags: List[str] = []

    def get_cases(self) -> List[BenchmarkCase]:
        return [
            DecompressBenchmarkCase(command_line_args=["-d", file] + self.additional_flags,
                                    results_columns={"type": self.type, "file": file,
                                                     "additional_flags": ' '.join(self.additional_flags)},
                                    repeats=self.repeats)
            for file in self.inputs
        ]


class BenchmarkCompressFile(BenchmarkTypeBase):
    type: Literal['compress-files']
    inputs: List[str]
    additional_flags: List[str] = []

    def get_cases(self) -> List[BenchmarkCase]:
        return [
            CompressDecompressBenchmarkCase(command_line_args=[file] + self.additional_flags,
                                    results_columns={"type": self.type, "file": file,
                                                     "additional_flags": ' '.join(self.additional_flags)},
                                    repeats=self.repeats)
            for file in self.inputs
        ]



BenchmarkAction = Union[BenchmarkDecompressFile, BenchmarkCompressFile]


class BenchmarkConfig(BaseModel):
    benchmarks: List[BenchmarkAction]
    realtime: bool = True
    evaluation_time: int = 3
    git_labels: Optional[List[str]] = None
    cpu: Optional[int] = None
    compilers: Optional[List[str]] = None
    cflags: Optional[List[str]] = None
