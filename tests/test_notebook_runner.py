"""전체 갱신 중 통합 실행 실패가 기존 제출물을 덮어쓰지 않는지 확인한다."""
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import nbformat

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import run_notebook


class PublicationTests(unittest.TestCase):
    def test_integrated_failure_preserves_all_existing_notebooks(self):
        self.check_publication(fail_integrated=True)

    def test_all_three_publish_only_after_integrated_execution(self):
        self.check_publication(fail_integrated=False)

    def check_publication(self, *, fail_integrated):
        old_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "reports").mkdir()
            names = (*run_notebook.NAMES, "job_platform_integrated.ipynb")
            originals = {name: ("preserved " + name).encode() for name in names}
            for name, content in originals.items():
                (root / name).write_bytes(content)

            def notebook():
                cell = nbformat.v4.new_code_cell("print('fixture')", metadata={"analysis_chart": True})
                return nbformat.v4.new_notebook(cells=[cell], metadata={"jobtrend": {"minimum_interpretations": 1}})

            def build(output_path):
                nbformat.write(notebook(), output_path)

            class Client:
                def __init__(self, nb, **kwargs):
                    self.nb = nb

                def execute(self):
                    self.nb.cells[0].execution_count = 1
                    self.nb.cells[0].outputs = [nbformat.v4.new_output(
                        "display_data", data={"text/markdown": "해석: 실행기 제어 흐름 시험", "image/png": "AA=="})]

            def integrate(executed):
                self.assertEqual(set(executed), set(run_notebook.NAMES))
                for name, content in originals.items():
                    self.assertEqual((root / name).read_bytes(), content)
                if fail_integrated:
                    raise RuntimeError("integrated kernel failed")
                nb = notebook()
                Client(nb).execute()
                return nb, {"execution_verified": True}

            modules = {
                "build_etl_notebook": types.SimpleNamespace(build_etl=build),
                "build_eda_notebook": types.SimpleNamespace(build_eda=build),
                "build_integrated_notebook": types.SimpleNamespace(
                    NAME=names[-1], prepare_integrated=integrate, write_report=lambda result: None),
            }
            try:
                with patch.object(run_notebook, "ROOT", root), patch.object(run_notebook, "NotebookClient", Client), patch.dict(sys.modules, modules):
                    if fail_integrated:
                        with self.assertRaisesRegex(RuntimeError, "integrated kernel failed"):
                            run_notebook.run()
                        for name, content in originals.items():
                            self.assertEqual((root / name).read_bytes(), content)
                        self.assertFalse((root / "reports/notebook_validation.json").exists())
                    else:
                        run_notebook.run()
                        for name in names:
                            nb = nbformat.read(root / name, as_version=4)
                            self.assertEqual(nb.cells[0].execution_count, 1)
                            self.assertTrue(nb.cells[0].outputs)
                        report = json.loads((root / "reports/notebook_validation.json").read_text())
                        self.assertEqual(set(report["notebooks"]), set(names))
            finally:
                import os
                os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
