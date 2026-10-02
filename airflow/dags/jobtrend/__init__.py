"""jobtrend — 채용 공고 10분 스냅샷 수집·적재 패키지.

노트북 job_platform_pipeline.ipynb가 %%writefile로 만들고, 배포 게이트를 거쳐 Airflow dags 폴더에 복사합니다.
Airflow·pandas에 의존하지 않는 순수 파이썬이라 노트북 커널(3.12)과 Airflow 컨테이너(3.13)가 같은 코드를 import합니다.
"""
import hashlib
import pathlib

_PKG_DIR = pathlib.Path(__file__).resolve().parent


def _code_version() -> str:
    """패키지 안 .py·.sql 파일 내용의 sha256 앞 12자.

    노트북과 컨테이너가 같은 코드를 쓰고 있는지 대조하는 지문입니다(crawl_run.code_version에 남김).
    """
    digest = hashlib.sha256()
    for path in sorted(_PKG_DIR.rglob("*")):
        if path.suffix in (".py", ".sql") and "__pycache__" not in path.parts:
            digest.update(path.relative_to(_PKG_DIR).as_posix().encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


CODE_VERSION = _code_version()
