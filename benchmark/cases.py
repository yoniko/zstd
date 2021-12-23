from typing import List, Dict, Union
from pydantic import BaseModel


class BenchmarkCase(BaseModel):
    command_line_args: List[str]
    results_columns: Dict[str, str]

    def process_output(self, stdout: str) -> Dict[str, Union[str, int, float]]:
        raise NotImplementedError()


class CompressDecompressBenchmarkCase(BenchmarkCase):
    def process_output(self, stdout: str) -> Dict[str, Union[str, int, float]]:
        output = stdout.split(' ')
        idx = [i for i, d in enumerate(output) if d == "MB/s"]
        return {"compression_speed": float(output[idx[0] - 1]),
                "decompression_speed": float(output[idx[1] - 1])
                }


class DecompressBenchmarkCase(CompressDecompressBenchmarkCase):
    def process_output(self, stdout: str) -> Dict[str, Union[str, int, float]]:
        return {
            "decompression_speed": super(DecompressBenchmarkCase, self).process_output(stdout)["decompression_speed"]}
