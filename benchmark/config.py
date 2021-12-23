from typing import List, Optional, Literal, Union
from pydantic import BaseModel

from cases import BenchmarkCase, DecompressBenchmarkCase


class BenchmarkTypeBase(BaseModel):
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
                                                     "additional_flags": ' '.join(self.additional_flags)})
            for file in self.inputs
        ]


BenchmarkAction = Union[BenchmarkDecompressFile]


class BenchmarkConfig(BaseModel):
    benchmarks: List[BenchmarkAction]
    realtime: bool = True
    evaluation_time: int = 3
    # hashes: Optional[List[str]] = None
    cpu: Optional[int] = None
    compilers: List[str] = ['']
    cflags: Optional[List[str]] = None
