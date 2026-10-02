"""게시 노트북에는 코드·결과와 커널 실행에 필요한 정보만 남긴다."""
from copy import deepcopy

import nbformat


def project_notebook(notebook):
    result = deepcopy(notebook)
    result.metadata = nbformat.NotebookNode({
        "kernelspec": dict(notebook.metadata.get("kernelspec", {
            "display_name": "Python 3 (ipykernel)", "language": "python", "name": "python3"})),
        "language_info": {"name": "python"},
    })
    for cell in result.cells:
        cell.metadata = nbformat.NotebookNode()
        for output in cell.get("outputs", []):
            if "metadata" in output:
                output.metadata = nbformat.NotebookNode()
    nbformat.validate(result)
    return result
