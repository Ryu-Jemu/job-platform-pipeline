"""분리 노트북의 코드를 보존해 통합본을 만들고 한 커널에서 전체 실행한다.

python scripts/build_integrated_notebook.py
원본 ETL·EDA 파일은 수정하지 않으며, 통과한 통합본만 원자적으로 교체한다.
"""
from copy import deepcopy
from pathlib import Path
import hashlib
import json
import os
import re
import time

import nbformat
from nbclient import NotebookClient
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
SOURCE_NAMES = ("job_platform_etl.ipynb", "job_platform_eda.ipynb")
NAME = "job_platform_integrated.ipynb"
REPORT = "integrated_notebook_validation.json"

INTRO = """# 채용 공고 트렌드 · ETL·EDA 통합 제출본

채용 웹 원천을 수집·정규화·통합하고 BE·DA·DE의 공고 구성과 기술 키워드를 분석한다.
이 파일은 ETL과 EDA의 코드를 순서대로 실행하는 단일 제출본이다.
[ETL 분리본](job_platform_etl.ipynb)과 [EDA 분리본](job_platform_eda.ipynb)은 별도로 유지한다.

평가자는 아래 링크로 구현·출력·해석을 함께 확인할 수 있다.
세부 채점 기준과 개선 계획은 [평가표](docs/NOTION-RUBRIC.md)에 있다.

| 번호 | 평가 항목 | 통합본의 확인 위치 |
|---|---|---|
| 1 | 동시 수집 | [ETL 2: 실행 코드·비교·Gantt](#etl-2) |
| 2 | 자동 재시도 | [ETL 2: 백오프·실패 응답 시험](#etl-2) |
| 3 | 데이터 적합성 | [ETL 1: 분할 요청·고유키·수집 범위](#etl-1) |
| 4 | 원본·정제 분리 | [ETL 3: 테이블과 원본 계보](#etl-3) |
| 5 | 중복 없는 재실행 | [ETL 4: 두 번 적재 전후 행 수](#etl-4) |
| 6 | 목적 있는 SQL | [EDA 2: 목적·윈도우 SQL·결과](#eda-2) |
| 7 | 실제 자료 시각화 | [EDA 3: 기업·조건](#eda-3), [EDA 5: 기술·역량](#eda-5) |
| 8 | 결측·분포 해석 | [EDA 1: 결측](#eda-1), [EDA 7: 분포·이상치](#eda-7) |
| 9 | 코드·실행 결과 보존 | [전체 실행 요약](#integrated-results) |
| 10 | 주제에 맞춘 구현 | [ETL 5: 중복 통합](#etl-5), [EDA 5: 키워드 분석](#eda-5) |

**실행 안내:** 저장된 출력으로 결과를 읽을 수 있다. 재실행은 프로젝트 루트에서
`python scripts/build_integrated_notebook.py`를 사용한다. `.env`의 DB 연결과 기존 Airflow 환경이 필요하다.
저장 원본을 재처리하며 새 웹 수집을 요청하지 않는다. 동시성 시간 비교는 고정 응답 시험이고,
실제 웹의 처리 속도를 의미하지 않는다. 수집 범위와 자료 기준은 각 파트의 출력에 표시한다.
"""


def _code_digest(cells):
    payload = [c.source for c in cells if c.cell_type == "code"]
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()


def compose(notebooks):
    """코드 셀은 그대로 복제하고 제목·목차·상호 링크만 단일 문서에 맞춘다."""
    cells = [nbformat.v4.new_markdown_cell(INTRO.strip(), id="integrated-intro")]
    source_info = []
    for name, prefix in zip(SOURCE_NAMES, ("etl", "eda")):
        source = notebooks[name]
        source_info.append({"file": name, "code_sha256": _code_digest(source.cells),
                            "code_cells": sum(c.cell_type == "code" for c in source.cells)})
        for index, original in enumerate(source.cells):
            cell = deepcopy(original)
            cell.id = f"{prefix}-{index + 1}"
            cell.metadata["source_notebook"] = name
            cell.metadata["source_cell"] = index + 1
            if cell.cell_type == "code":
                cell.execution_count = None
                cell.outputs = []
            elif cell.cell_type == "markdown":
                text = cell.source
                text = text.replace("(job_platform_etl.ipynb)", "(#etl)")
                text = text.replace("(job_platform_eda.ipynb)", "(#eda)")
                if index == 0:
                    # 평가표는 통합본 첫머리에서 한 번만 제공한다.
                    if prefix == "etl":
                        text = text.split("|[Notion 평가 기준]", 1)[0].rstrip()
                    else:
                        text = "\n\n".join(p for p in text.split("\n\n")
                                            if not p.startswith("사람인·잡코리아·인크루트·링커리어의 실제 저장 자료"))
                    text = re.sub(r"^# .*", "## " + ("A. ETL" if prefix == "etl" else "B. EDA"), text, count=1)
                    anchor = prefix
                else:
                    section = re.match(r"## (\d+)\. (.*)", text)
                    if section:
                        number, title = section.groups()
                        anchor = f"{prefix}-{number}"
                        text = re.sub(r"^## \d+\. .*", f"### {prefix.upper()} {number}. {title}", text, count=1)
                        if prefix == "etl" and number == "8":
                            text = text.replace(
                                "재현은 프로젝트 폴더에서 `python scripts/build_etl_notebook.py`로 생성하고 두 노트북의 전체 셀을 새 커널에서 실행한다.",
                                "통합본 재현은 프로젝트 폴더에서 `python scripts/build_integrated_notebook.py`로 전체 셀을 한 새 커널에서 실행한다. 분리본 두 파일은 유지한다.")
                    else:
                        anchor = f"{prefix}-results"
                        if prefix == "eda":
                            text = """### EDA 결과와 재실행

[전체 공고 웹 화면](web/index.html) · [평가표·개선 계획](docs/NOTION-RUBRIC.md)

집계 표·그림·해석은 위 실행 출력에 보존했다. 통합·원천 공고, 상관 통계와 IQR 경계의 CSV는
재실행하면 `reports/`에 생성된다. 수집 종료 후 자동 마무리 과정에서 분리본과 통합본, 웹 자료를 갱신한다.
실제 종료 상태는 [finalization.json](reports/finalization.json)에서 확인한다.

이 자료는 하루의 부분 관측·플랫폼 직무 코드의 범위 차이·상세 수집 한도·현재 목록에 남은 공고의
편향을 포함한다. 기업별 공고 수는 고용 인원이 아니며 중복 통합과 키워드 검출은 추정이다."""
                cell.source = f'<a id="{anchor}"></a>\n\n{text}'
            cells.append(cell)
    metadata = deepcopy(notebooks[SOURCE_NAMES[0]].metadata)
    metadata["jobtrend"] = {"kind": "integrated", "generated_by": "scripts/build_integrated_notebook.py",
                            "execution_verified": False, "minimum_interpretations": 16,
                            "source_notebooks": source_info}
    nb = nbformat.v4.new_notebook(cells=cells, metadata=metadata)
    nbformat.validate(nb)
    expected = [c.source for name in SOURCE_NAMES for c in notebooks[name].cells if c.cell_type == "code"]
    assert [c.source for c in nb.cells if c.cell_type == "code"] == expected
    return nb


def prepare_integrated(notebooks=None):
    """한 커널에서 실행·검사한 노트북과 요약을 반환한다. 분리본은 쓰지 않는다."""
    started = time.monotonic()
    notebooks = notebooks or {name: nbformat.read(ROOT / name, as_version=4) for name in SOURCE_NAMES}
    nb = compose(notebooks)
    try:
        NotebookClient(nb, timeout=600, kernel_name="python3",
                       resources={"metadata": {"path": str(ROOT)}}).execute()
    except Exception:
        nbformat.write(nb, ROOT / "reports" / NAME.replace(".ipynb", "_failed.ipynb"))
        raise
    code = [c for c in nb.cells if c.cell_type == "code"]
    assert [c.execution_count for c in code] == list(range(1, len(code) + 1))
    assert all(c.outputs for c in code), "실행 출력이 없는 코드 셀"
    errors = [o for c in code for o in c.outputs if o.output_type == "error"]
    assert not errors
    chart_cells = [c for c in code if c.metadata.get("analysis_chart")]
    assert chart_cells and all(any("image/png" in o.get("data", {}) for o in c.outputs) for c in chart_cells)
    images = sum("image/png" in o.get("data", {}) for c in code for o in c.outputs)
    notes = sum(any(s in str(o.get("data", {}).get("text/markdown", "")) for s in ("✍️", "해석:"))
                for c in code for o in c.outputs)
    assert notes >= nb.metadata.jobtrend.minimum_interpretations
    for name in SOURCE_NAMES:
        copied = [c for c in code if c.metadata.source_notebook == name]
        assert _code_digest(copied) == _code_digest(notebooks[name].cells), name
    serialized = nbformat.writes(nb)
    for env in (ROOT / ".env", ROOT / "airflow/.env"):
        if env.exists():
            for key, value in dotenv_values(env).items():
                assert not (value and len(value) > 12 and value in serialized), f"secret leakage detected: {key}"
    etl = json.loads((ROOT / "reports/etl_validation.json").read_text())
    eda = json.loads((ROOT / "reports/analysis_validation.json").read_text())
    nb.metadata.jobtrend.execution_verified = True
    summary = {"execution_verified": True, "execution_mode": "single_fresh_kernel",
               "code_cells": len(code), "execution_count_contiguous": True, "errors": len(errors),
               "png_figures": images, "interpretations": notes, "source_code_preserved": True,
               "source_notebooks": list(nb.metadata.jobtrend.source_notebooks),
               "etl_asof": etl["asof"], "eda_asof": eda["asof"], "catalog": eda["catalog"],
               "elapsed_s": round(time.monotonic() - started, 2)}
    nb.cells.append(nbformat.v4.new_markdown_cell(
        '<a id="integrated-results"></a>\n\n## 전체 실행 요약\n\n'
        f'원본 코드 {len(code)}셀을 ETL → EDA 순서로 한 새 커널에서 실행했다. '
        f'실행 번호는 1–{len(code)}이며 오류 출력은 {len(errors)}개다. '
        f'PNG 그림 {images}개와 해석 출력 {notes}개를 코드와 함께 보존했다.\n\n'
        '이 요약은 실행기가 완료된 노트북을 검사한 결과다. '
        '[실행 기록](reports/integrated_notebook_validation.json)과 '
        '[평가표·개선 계획](docs/NOTION-RUBRIC.md)에서 근거를 확인할 수 있다. '
        '원본 ETL·EDA의 코드 셀은 수정하지 않았으며, 통합본의 결과는 이번 실행의 DB 자료를 반영한다.',
        id="integrated-results"))
    nbformat.validate(nb)
    return nb, summary


def write_report(summary):
    target = ROOT / "reports" / REPORT
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, target)


def run():
    before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCE_NAMES}
    nb, summary = prepare_integrated()
    after = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCE_NAMES}
    assert before == after, "통합 실행 중 분리본 파일이 변경됨"
    summary["split_files_unchanged"] = True
    summary["split_file_sha256"] = before
    tmp = ROOT / "reports" / NAME.replace(".ipynb", ".verified.tmp")
    nbformat.write(nb, tmp)
    os.replace(tmp, ROOT / NAME)
    write_report(summary)
    print(json.dumps(summary, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    run()
