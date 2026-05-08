"""
데이터 모니터링 관리자 도구
실시간 콘솔에서 저장된 데이터 상태를 조회합니다.
"""

import sqlite3
import os
import sys
import time
import threading
import json
from datetime import datetime, timedelta
from collections import defaultdict
import random  # 데모 데이터 생성용


# ─── ANSI 컬러 코드 ────────────────────────────────────────────────────────────
class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    BG_DARK = "\033[40m"
    CLEAR   = "\033[2J\033[H"


# ─── 데이터베이스 관리 ─────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "monitor.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                source    TEXT    NOT NULL,
                level     TEXT    NOT NULL DEFAULT 'INFO',
                message   TEXT    NOT NULL,
                payload   TEXT,
                created_at TEXT   NOT NULL DEFAULT (datetime('now','localtime'))
            );

            CREATE TABLE IF NOT EXISTS metrics (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT    NOT NULL,
                value      REAL    NOT NULL,
                unit       TEXT,
                recorded_at TEXT  NOT NULL DEFAULT (datetime('now','localtime'))
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT    NOT NULL,
                severity    TEXT    NOT NULL DEFAULT 'WARNING',
                status      TEXT    NOT NULL DEFAULT 'OPEN',
                description TEXT,
                created_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
                resolved_at TEXT
            );
        """)


def seed_demo_data():
    """데모 데이터가 없을 때 초기 데이터를 삽입합니다."""
    with get_conn() as conn:
        count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        if count > 0:
            return

        sources  = ["API서버", "DB서버", "캐시서버", "배치작업", "외부연동"]
        levels   = ["INFO", "INFO", "INFO", "WARNING", "ERROR"]
        messages = [
            "요청 처리 완료",
            "데이터 동기화 성공",
            "헬스체크 정상",
            "응답 지연 감지 (>500ms)",
            "연결 실패 — 재시도 중",
            "캐시 히트율 저하",
            "배치 작업 완료",
            "메모리 사용량 증가",
        ]

        now = datetime.now()
        for i in range(50):
            ts = (now - timedelta(minutes=random.randint(0, 120))).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                "INSERT INTO events (source, level, message, created_at) VALUES (?,?,?,?)",
                (random.choice(sources), random.choice(levels), random.choice(messages), ts),
            )

        metrics_data = [
            ("CPU 사용률", random.uniform(20, 80), "%"),
            ("메모리 사용률", random.uniform(30, 70), "%"),
            ("디스크 사용률", random.uniform(40, 60), "%"),
            ("초당 요청수", random.uniform(50, 300), "req/s"),
            ("평균 응답시간", random.uniform(50, 400), "ms"),
            ("DB 쿼리/s", random.uniform(100, 500), "q/s"),
            ("캐시 히트율", random.uniform(70, 99), "%"),
            ("에러율", random.uniform(0, 5), "%"),
        ]
        for name, value, unit in metrics_data:
            conn.execute(
                "INSERT INTO metrics (name, value, unit) VALUES (?,?,?)",
                (name, round(value, 2), unit),
            )

        alerts_data = [
            ("API 서버 응답 지연", "WARNING", "OPEN",  "평균 응답시간이 400ms를 초과했습니다."),
            ("DB 연결 풀 부족", "CRITICAL","OPEN",  "연결 풀 사용률 95% 이상입니다."),
            ("캐시 만료 급증", "WARNING", "RESOLVED", "Redis 키 만료로 캐시 히트율이 하락했습니다."),
            ("배치 실패",      "ERROR",   "OPEN",  "야간 배치 작업이 3회 실패했습니다."),
        ]
        for title, severity, status, desc in alerts_data:
            conn.execute(
                "INSERT INTO alerts (title, severity, status, description) VALUES (?,?,?,?)",
                (title, severity, status, desc),
            )


# ─── 실시간 데이터 갱신 스레드 ────────────────────────────────────────────────
_live_data: dict = {}
_live_lock  = threading.Lock()
_stop_live  = threading.Event()


def _live_worker():
    """백그라운드에서 DB 메트릭을 주기적으로 갱신하고 새 이벤트를 시뮬레이션합니다."""
    sources  = ["API서버", "DB서버", "캐시서버", "배치작업", "외부연동"]
    levels   = ["INFO", "INFO", "WARNING", "ERROR"]
    messages = [
        "요청 처리 완료", "헬스체크 정상", "응답 지연 감지", "연결 재시도", "데이터 동기화",
    ]
    while not _stop_live.is_set():
        try:
            with get_conn() as conn:
                # 새 이벤트 삽입
                if random.random() < 0.6:
                    conn.execute(
                        "INSERT INTO events (source, level, message) VALUES (?,?,?)",
                        (random.choice(sources), random.choice(levels), random.choice(messages)),
                    )
                # 메트릭 업데이트
                rows = conn.execute("SELECT DISTINCT name, unit FROM metrics").fetchall()
                for row in rows:
                    if row["name"] == "CPU 사용률":
                        val = round(random.uniform(20, 90), 2)
                    elif row["name"] == "메모리 사용률":
                        val = round(random.uniform(30, 75), 2)
                    elif row["name"] == "초당 요청수":
                        val = round(random.uniform(50, 350), 2)
                    elif row["name"] == "평균 응답시간":
                        val = round(random.uniform(40, 500), 2)
                    elif row["name"] == "에러율":
                        val = round(random.uniform(0, 8), 2)
                    else:
                        val = round(random.uniform(0, 100), 2)
                    conn.execute(
                        "INSERT INTO metrics (name, value, unit) VALUES (?,?,?)",
                        (row["name"], val, row["unit"]),
                    )

                # 캐시 갱신
                total   = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                errors  = conn.execute("SELECT COUNT(*) FROM events WHERE level='ERROR'").fetchone()[0]
                warns   = conn.execute("SELECT COUNT(*) FROM events WHERE level='WARNING'").fetchone()[0]
                open_al = conn.execute("SELECT COUNT(*) FROM alerts WHERE status='OPEN'").fetchone()[0]

            with _live_lock:
                _live_data.update({
                    "total_events": total,
                    "error_events": errors,
                    "warn_events":  warns,
                    "open_alerts":  open_al,
                    "updated_at":   datetime.now().strftime("%H:%M:%S"),
                })
        except Exception:
            pass
        _stop_live.wait(3)


# ─── 출력 유틸리티 ────────────────────────────────────────────────────────────
def clear():
    print(C.CLEAR, end="")


def header(title: str):
    w = 70
    print(f"{C.CYAN}{C.BOLD}{'━' * w}{C.RESET}")
    print(f"{C.CYAN}{C.BOLD}  {title}{C.RESET}")
    print(f"{C.CYAN}{'━' * w}{C.RESET}")


def footer():
    print(f"\n{C.DIM}{'─' * 70}{C.RESET}")
    print(f"{C.DIM}  [0] 뒤로  │  입력 후 Enter{C.RESET}")


def level_color(level: str) -> str:
    return {
        "ERROR":    C.RED,
        "WARNING":  C.YELLOW,
        "INFO":     C.GREEN,
        "CRITICAL": C.MAGENTA,
    }.get(level, C.WHITE)


def severity_color(s: str) -> str:
    return {
        "CRITICAL": C.MAGENTA,
        "ERROR":    C.RED,
        "WARNING":  C.YELLOW,
    }.get(s, C.WHITE)


def status_color(s: str) -> str:
    return (C.GREEN if s == "RESOLVED" else C.RED)


def bar(value: float, total: float = 100, width: int = 20) -> str:
    pct   = min(value / total, 1.0)
    filled = int(pct * width)
    color = C.GREEN if pct < 0.6 else (C.YELLOW if pct < 0.85 else C.RED)
    return f"{color}{'█' * filled}{'░' * (width - filled)}{C.RESET} {value:.1f}%"


def prompt(msg: str = "선택") -> str:
    try:
        return input(f"\n{C.BOLD}  {msg} › {C.RESET}").strip()
    except (KeyboardInterrupt, EOFError):
        return "0"


# ─── 화면 구성 ────────────────────────────────────────────────────────────────
def screen_dashboard():
    """메인 대시보드 — 실시간 갱신"""
    while True:
        clear()
        header("실시간 대시보드")
        with _live_lock:
            d = dict(_live_data)

        ts = d.get("updated_at", "—")
        print(f"  {C.DIM}마지막 갱신: {ts}  (자동 갱신 3초){C.RESET}\n")

        # 요약 카드
        cards = [
            ("전체 이벤트",   d.get("total_events", 0), C.CYAN),
            ("에러 이벤트",   d.get("error_events",  0), C.RED),
            ("경고 이벤트",   d.get("warn_events",   0), C.YELLOW),
            ("미해결 알림",   d.get("open_alerts",   0), C.MAGENTA),
        ]
        for label, val, color in cards:
            print(f"  {color}{C.BOLD}{val:>6}{C.RESET}  {label}")

        # 최신 메트릭
        print(f"\n  {C.BOLD}[ 최신 메트릭 ]{C.RESET}")
        try:
            with get_conn() as conn:
                rows = conn.execute("""
                    SELECT name, value, unit
                    FROM metrics
                    WHERE id IN (
                        SELECT MAX(id) FROM metrics GROUP BY name
                    )
                    ORDER BY name
                """).fetchall()
            for row in rows:
                name, value, unit = row["name"], row["value"], row["unit"]
                if unit == "%":
                    line = f"  {name:<16} {bar(value)}"
                else:
                    print(f"  {name:<16} {C.CYAN}{value:>8.1f}{C.RESET} {unit}")
                    continue
                print(line)
        except Exception as e:
            print(f"  {C.RED}메트릭 조회 오류: {e}{C.RESET}")

        footer()
        print(f"  {C.DIM}[r] 수동 갱신  [0] 뒤로{C.RESET}")
        choice = prompt()
        if choice == "0":
            return
        # 그 외 입력은 그냥 갱신


def screen_events():
    """이벤트 로그 조회"""
    page = 0
    page_size = 15
    filter_level = None

    while True:
        clear()
        header("이벤트 로그")

        levels_filter = f"  필터: {C.YELLOW}{filter_level}{C.RESET}" if filter_level else f"  필터: {C.DIM}없음{C.RESET}"
        print(levels_filter)

        try:
            with get_conn() as conn:
                where = f"WHERE level='{filter_level}'" if filter_level else ""
                total = conn.execute(f"SELECT COUNT(*) FROM events {where}").fetchone()[0]
                rows = conn.execute(f"""
                    SELECT id, source, level, message, created_at
                    FROM events {where}
                    ORDER BY id DESC
                    LIMIT {page_size} OFFSET {page * page_size}
                """).fetchall()
        except Exception as e:
            print(f"  {C.RED}조회 오류: {e}{C.RESET}")
            prompt(); return

        total_pages = max(1, (total + page_size - 1) // page_size)
        print(f"  총 {C.BOLD}{total}{C.RESET}건  │  페이지 {page+1}/{total_pages}\n")

        print(f"  {C.BOLD}{'ID':>5}  {'시각':<19}  {'소스':<10}  {'레벨':<8}  메시지{C.RESET}")
        print(f"  {'─'*5}  {'─'*19}  {'─'*10}  {'─'*8}  {'─'*25}")
        for row in rows:
            lc = level_color(row["level"])
            print(
                f"  {C.DIM}{row['id']:>5}{C.RESET}  "
                f"{row['created_at']:<19}  "
                f"{row['source']:<10}  "
                f"{lc}{row['level']:<8}{C.RESET}  "
                f"{row['message']}"
            )

        print(f"\n  {C.DIM}[n] 다음  [p] 이전  [f] 레벨 필터  [c] 필터 해제  [0] 뒤로{C.RESET}")
        choice = prompt()
        if choice == "0":
            return
        elif choice == "n":
            if (page + 1) < total_pages:
                page += 1
        elif choice == "p":
            if page > 0:
                page -= 1
        elif choice == "f":
            clear()
            header("레벨 필터 선택")
            print("  [1] INFO\n  [2] WARNING\n  [3] ERROR\n  [0] 뒤로")
            fc = prompt()
            mapping = {"1": "INFO", "2": "WARNING", "3": "ERROR"}
            if fc in mapping:
                filter_level = mapping[fc]
                page = 0
        elif choice == "c":
            filter_level = None
            page = 0


def screen_metrics():
    """메트릭 이력 조회"""
    while True:
        clear()
        header("메트릭 이력")

        try:
            with get_conn() as conn:
                names = [r[0] for r in conn.execute("SELECT DISTINCT name FROM metrics ORDER BY name").fetchall()]
        except Exception as e:
            print(f"  {C.RED}오류: {e}{C.RESET}")
            prompt(); return

        for i, name in enumerate(names, 1):
            print(f"  [{i}] {name}")
        print(f"\n  [0] 뒤로")
        choice = prompt("메트릭 선택")
        if choice == "0":
            return
        try:
            idx = int(choice) - 1
            if idx < 0 or idx >= len(names):
                continue
        except ValueError:
            continue

        selected = names[idx]
        _show_metric_detail(selected)


def _show_metric_detail(name: str):
    """특정 메트릭의 최근 30개 이력과 간단한 ASCII 차트를 표시합니다."""
    while True:
        clear()
        header(f"메트릭 상세 — {name}")

        try:
            with get_conn() as conn:
                rows = conn.execute("""
                    SELECT value, unit, recorded_at
                    FROM metrics
                    WHERE name=?
                    ORDER BY id DESC
                    LIMIT 30
                """, (name,)).fetchall()
        except Exception as e:
            print(f"  {C.RED}오류: {e}{C.RESET}")
            prompt(); return

        if not rows:
            print("  데이터가 없습니다.")
            prompt(); return

        values = [r["value"] for r in rows]
        unit   = rows[0]["unit"]
        mn, mx, avg = min(values), max(values), sum(values) / len(values)

        print(f"  최소: {C.GREEN}{mn:.2f}{C.RESET} {unit}  "
              f"최대: {C.RED}{mx:.2f}{C.RESET} {unit}  "
              f"평균: {C.CYAN}{avg:.2f}{C.RESET} {unit}\n")

        # 간단한 수직 ASCII 차트
        chart_h = 8
        chart_w = min(len(values), 40)
        samples = list(reversed(values))[-chart_w:]
        range_  = mx - mn if mx != mn else 1

        for row_i in range(chart_h, 0, -1):
            threshold = mn + (row_i / chart_h) * range_
            line = ""
            for v in samples:
                line += (f"{C.CYAN}█{C.RESET}" if v >= threshold else " ")
            label = f"{threshold:>6.1f} │"
            print(f"  {C.DIM}{label}{C.RESET} {line}")
        print(f"  {' ' * 8}└{'─' * chart_w}")
        print(f"  {' ' * 9}{C.DIM}최근 {len(samples)}개 측정값{C.RESET}\n")

        # 테이블
        print(f"  {C.BOLD}{'#':>4}  {'값':>10}  {'측정시각'}{C.RESET}")
        print(f"  {'─'*4}  {'─'*10}  {'─'*19}")
        for i, row in enumerate(rows, 1):
            vc = C.RED if row["value"] == mx else (C.GREEN if row["value"] == mn else C.RESET)
            print(f"  {i:>4}  {vc}{row['value']:>10.2f}{C.RESET}  {row['recorded_at']}")

        footer()
        choice = prompt()
        if choice == "0":
            return


def screen_alerts():
    """알림 목록 조회 및 상태 변경"""
    while True:
        clear()
        header("알림 관리")

        try:
            with get_conn() as conn:
                rows = conn.execute("""
                    SELECT id, title, severity, status, description, created_at, resolved_at
                    FROM alerts
                    ORDER BY
                        CASE status WHEN 'OPEN' THEN 0 ELSE 1 END,
                        CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'ERROR' THEN 1 ELSE 2 END,
                        id DESC
                """).fetchall()
        except Exception as e:
            print(f"  {C.RED}오류: {e}{C.RESET}")
            prompt(); return

        for row in rows:
            sc = severity_color(row["severity"])
            stc = status_color(row["status"])
            print(
                f"  [{C.BOLD}{row['id']}{C.RESET}]  "
                f"{sc}{row['severity']:<8}{C.RESET}  "
                f"{stc}{row['status']:<8}{C.RESET}  "
                f"{row['title']}"
            )
            if row["description"]:
                print(f"        {C.DIM}{row['description']}{C.RESET}")

        print(f"\n  {C.DIM}[번호] 알림 상세/처리  [a] 새 알림 추가  [0] 뒤로{C.RESET}")
        choice = prompt()
        if choice == "0":
            return
        elif choice == "a":
            _add_alert()
        else:
            try:
                alert_id = int(choice)
                _show_alert_detail(alert_id)
            except ValueError:
                pass


def _show_alert_detail(alert_id: int):
    while True:
        clear()
        try:
            with get_conn() as conn:
                row = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
        except Exception:
            return
        if not row:
            print(f"  {C.RED}알림 ID {alert_id} 를 찾을 수 없습니다.{C.RESET}")
            prompt(); return

        sc  = severity_color(row["severity"])
        stc = status_color(row["status"])
        header(f"알림 상세 — #{row['id']}")
        print(f"  제목:      {C.BOLD}{row['title']}{C.RESET}")
        print(f"  심각도:    {sc}{row['severity']}{C.RESET}")
        print(f"  상태:      {stc}{row['status']}{C.RESET}")
        print(f"  설명:      {row['description'] or '—'}")
        print(f"  생성시각:  {row['created_at']}")
        print(f"  해결시각:  {row['resolved_at'] or '—'}")

        if row["status"] == "OPEN":
            print(f"\n  {C.DIM}[r] 해결 처리  [d] 삭제  [0] 뒤로{C.RESET}")
        else:
            print(f"\n  {C.DIM}[d] 삭제  [0] 뒤로{C.RESET}")

        choice = prompt()
        if choice == "0":
            return
        elif choice == "r" and row["status"] == "OPEN":
            with get_conn() as conn:
                conn.execute(
                    "UPDATE alerts SET status='RESOLVED', resolved_at=datetime('now','localtime') WHERE id=?",
                    (alert_id,),
                )
            print(f"  {C.GREEN}해결 처리되었습니다.{C.RESET}")
            time.sleep(1)
            return
        elif choice == "d":
            with get_conn() as conn:
                conn.execute("DELETE FROM alerts WHERE id=?", (alert_id,))
            print(f"  {C.YELLOW}알림이 삭제되었습니다.{C.RESET}")
            time.sleep(1)
            return


def _add_alert():
    clear()
    header("새 알림 추가")
    title = input("  제목: ").strip()
    if not title:
        return
    print("  심각도: [1] WARNING  [2] ERROR  [3] CRITICAL")
    sc = prompt("심각도")
    severity_map = {"1": "WARNING", "2": "ERROR", "3": "CRITICAL"}
    severity = severity_map.get(sc, "WARNING")
    desc = input("  설명 (선택): ").strip()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO alerts (title, severity, description) VALUES (?,?,?)",
            (title, severity, desc or None),
        )
    print(f"  {C.GREEN}알림이 추가되었습니다.{C.RESET}")
    time.sleep(1)


def screen_stats():
    """데이터 통계 요약"""
    clear()
    header("데이터 통계 요약")

    try:
        with get_conn() as conn:
            # 이벤트 통계
            total_ev = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            ev_by_level = conn.execute(
                "SELECT level, COUNT(*) as cnt FROM events GROUP BY level ORDER BY cnt DESC"
            ).fetchall()
            ev_by_source = conn.execute(
                "SELECT source, COUNT(*) as cnt FROM events GROUP BY source ORDER BY cnt DESC"
            ).fetchall()

            # 최근 1시간
            one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            recent = conn.execute(
                "SELECT COUNT(*) FROM events WHERE created_at >= ?", (one_hour_ago,)
            ).fetchone()[0]

            # 메트릭 통계
            metric_stats = conn.execute("""
                SELECT name, unit,
                       AVG(value) as avg_val,
                       MIN(value) as min_val,
                       MAX(value) as max_val,
                       COUNT(*) as cnt
                FROM metrics
                GROUP BY name
                ORDER BY name
            """).fetchall()

            # 알림 통계
            alert_open = conn.execute("SELECT COUNT(*) FROM alerts WHERE status='OPEN'").fetchone()[0]
            alert_total = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    except Exception as e:
        print(f"  {C.RED}오류: {e}{C.RESET}")
        prompt(); return

    print(f"  {C.BOLD}[ 이벤트 요약 ]{C.RESET}")
    print(f"  전체 이벤트: {C.CYAN}{total_ev}{C.RESET}건  │  최근 1시간: {C.CYAN}{recent}{C.RESET}건")

    print(f"\n  레벨별 분포:")
    for row in ev_by_level:
        pct  = row["cnt"] / total_ev * 100 if total_ev else 0
        lc   = level_color(row["level"])
        filled = int(pct / 5)
        print(f"    {lc}{row['level']:<8}{C.RESET} {row['cnt']:>5}건  {'█'*filled}{'░'*(20-filled)} {pct:.1f}%")

    print(f"\n  소스별 분포:")
    for row in ev_by_source:
        pct = row["cnt"] / total_ev * 100 if total_ev else 0
        print(f"    {row['source']:<12} {row['cnt']:>5}건  {pct:.1f}%")

    print(f"\n  {C.BOLD}[ 메트릭 요약 ]{C.RESET}")
    print(f"  {'이름':<16} {'평균':>8}  {'최소':>8}  {'최대':>8}  {'측정수':>6}  단위")
    print(f"  {'─'*16}  {'─'*8}  {'─'*8}  {'─'*8}  {'─'*6}  {'─'*5}")
    for row in metric_stats:
        print(
            f"  {row['name']:<16} {row['avg_val']:>8.2f}  "
            f"{row['min_val']:>8.2f}  {row['max_val']:>8.2f}  "
            f"{row['cnt']:>6}  {row['unit']}"
        )

    print(f"\n  {C.BOLD}[ 알림 요약 ]{C.RESET}")
    print(f"  전체: {alert_total}건  │  미해결: {C.RED}{alert_open}{C.RESET}건")

    footer()
    prompt()


def screen_db_info():
    """DB 파일 정보 및 테이블 레코드 수"""
    clear()
    header("DB 정보")

    try:
        size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        print(f"  경로: {C.DIM}{DB_PATH}{C.RESET}")
        print(f"  크기: {C.CYAN}{size:,}{C.RESET} bytes")

        with get_conn() as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
            print(f"\n  {C.BOLD}{'테이블':<20}  {'레코드 수':>10}{C.RESET}")
            print(f"  {'─'*20}  {'─'*10}")
            for t in tables:
                cnt = conn.execute(f"SELECT COUNT(*) FROM [{t[0]}]").fetchone()[0]
                print(f"  {t[0]:<20}  {cnt:>10,}")
    except Exception as e:
        print(f"  {C.RED}오류: {e}{C.RESET}")

    footer()
    prompt()


# ─── 메인 메뉴 ───────────────────────────────────────────────────────────────
MENU_ITEMS = [
    ("1", "실시간 대시보드",       screen_dashboard),
    ("2", "이벤트 로그 조회",      screen_events),
    ("3", "메트릭 이력 조회",      screen_metrics),
    ("4", "알림 관리",             screen_alerts),
    ("5", "데이터 통계 요약",      screen_stats),
    ("6", "DB 정보",               screen_db_info),
    ("0", "종료",                  None),
]


def main_menu():
    while True:
        clear()
        with _live_lock:
            d = dict(_live_data)

        print(f"{C.CYAN}{C.BOLD}")
        print("  ╔══════════════════════════════════════════════╗")
        print("  ║       데이터 모니터링 관리자 도구            ║")
        print("  ╚══════════════════════════════════════════════╝")
        print(C.RESET)

        # 상태 요약 (헤더)
        if d:
            ev_total = d.get("total_events", "—")
            errors   = d.get("error_events",  "—")
            warns    = d.get("warn_events",   "—")
            alerts   = d.get("open_alerts",   "—")
            ts       = d.get("updated_at",    "—")
            print(
                f"  이벤트 {C.CYAN}{ev_total}{C.RESET}  │  "
                f"에러 {C.RED}{errors}{C.RESET}  │  "
                f"경고 {C.YELLOW}{warns}{C.RESET}  │  "
                f"미해결 알림 {C.MAGENTA}{alerts}{C.RESET}  │  "
                f"{C.DIM}{ts}{C.RESET}\n"
            )

        for key, label, _ in MENU_ITEMS:
            if key == "0":
                print(f"  {C.DIM}[{key}] {label}{C.RESET}")
            else:
                print(f"  {C.BOLD}[{key}]{C.RESET} {label}")

        choice = prompt("메뉴 선택")
        for key, _, fn in MENU_ITEMS:
            if choice == key:
                if fn is None:
                    return
                fn()
                break


# ─── 진입점 ──────────────────────────────────────────────────────────────────
def main():
    # Windows 콘솔 ANSI 활성화
    if sys.platform == "win32":
        os.system("")

    print("  데이터베이스 초기화 중...")
    init_db()
    seed_demo_data()

    # 실시간 갱신 스레드 시작
    worker = threading.Thread(target=_live_worker, daemon=True)
    worker.start()
    time.sleep(0.5)  # 초기 데이터 로딩 대기

    try:
        main_menu()
    finally:
        _stop_live.set()
        clear()
        print(f"  {C.GREEN}모니터링 도구를 종료합니다.{C.RESET}\n")


if __name__ == "__main__":
    main()
