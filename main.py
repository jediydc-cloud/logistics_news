"""
국내외 물류 뉴스 자동 브리핑 및 콘텐츠 아이디어 도출 시스템
GitHub Actions 실행용 스크립트 (배치 처리 버전 — API 호출 최소화)
"""

import os
import re
import json
import time
import warnings
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from bs4 import BeautifulSoup
import feedparser
from langdetect import detect, DetectorFactory
from dateutil import parser as dateparser
from google import genai

# ──────────────────────────────────────────
# 기본 설정
# ──────────────────────────────────────────
warnings.filterwarnings("ignore")
DetectorFactory.seed = 0
KST = ZoneInfo("Asia/Seoul")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
if not GEMINI_API_KEY:
    raise EnvironmentError("❌ GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")

# 워크플로에서 별도 지정하지 않으면 현재 운영 기본값 사용
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip()

client = genai.Client(api_key=GEMINI_API_KEY)

TOP_NEWS_COUNT = 5
CANDIDATE_POOL_SIZE = 10
CANDIDATE_MIN_DOMESTIC = 3
CANDIDATE_MIN_GLOBAL = 3
FINAL_MIN_DOMESTIC = 2
FINAL_MIN_GLOBAL = 2
CONTENT_IDEA_COUNT = 3
MAX_ARTICLES_PER_FEED = 12

GEMINI_RETRIES = 3
GEMINI_WAIT_SECONDS = 18

OUTPUT_PATH = "docs/data.json"

# ──────────────────────────────────────────
# RSS 피드 정의
# ──────────────────────────────────────────
RSS_FEEDS = [
    {
        "source": "물류신문",
        "region": "국내",
        "default_lang": "ko",
        "url": "https://www.klnews.co.kr/rss/S1N9.xml"
    },
    {
        "source": "Google News Korea",
        "region": "국내",
        "default_lang": "ko",
        "url": "https://news.google.com/rss?q=%ED%95%9C%EA%B5%AD%20%EB%AC%BC%EB%A5%98&hl=ko&gl=KR&ceid=KR%3Ako"
    },
    {
        "source": "Google News Global",
        "region": "해외",
        "default_lang": "en",
        "url": "https://news.google.com/rss/search?q=logistics+shipping+freight&hl=en&gl=US&ceid=US:en"
    },
    {
        "source": "Logistics Management",
        "region": "해외",
        "default_lang": "en",
        "url": "https://feeds.feedburner.com/logisticsmgmt/latest"
    }
]

# ──────────────────────────────────────────
# 카테고리 / 가중치 설정
# ──────────────────────────────────────────
CATEGORY_RULES = {
    "항만·해운": ["항만", "항구", "해운", "선사", "해상", "컨테이너", "선박", "port", "shipping", "ocean", "vessel", "container", "maritime"],
    "항공·운송": ["항공", "화물", "운송", "배송", "택배", "truck", "trucking", "delivery", "freight", "transport", "parcel"],
    "창고·물류센터": ["창고", "물류센터", "풀필먼트", "센터", "warehouse", "fulfillment", "distribution center", "dc"],
    "자동화·기술": ["로봇", "자동화", "ai", "rfid", "센서", "디지털", "플랫폼", "robot", "automation", "technology", "tech", "software"],
    "정책·공공": ["정부", "정책", "법안", "장관", "세관", "공사", "공공기관", "규제", "ministry", "government", "policy", "customs", "authority", "regulation"],
    "안전·리스크": ["파업", "지연", "사고", "리스크", "중단", "위험", "분실", "손실", "delay", "risk", "strike", "disruption", "accident", "loss"],
    "투자·경영": ["투자", "실적", "인수", "확장", "수익", "전략", "경영", "investment", "acquisition", "earnings", "profit", "expansion", "strategy"]
}

VALID_CATEGORIES = set(CATEGORY_RULES.keys()) | {"기타"}

SOURCE_WEIGHT = {
    "물류신문": 2.0,
    "Google News Korea": 1.6,
    "Google News Global": 1.6,
    "Logistics Management": 2.0
}

CATEGORY_WEIGHT = {
    "정책·공공": 2.3,
    "안전·리스크": 2.5,
    "자동화·기술": 2.2,
    "항만·해운": 1.9,
    "항공·운송": 1.9,
    "창고·물류센터": 1.8,
    "투자·경영": 1.8,
    "기타": 1.0
}

BAD_UI_PHRASES = [
    "[번역 대기]",
    "[번역 필요]",
    "자동 fallback",
    "분석 대기",
    "요약 생성 실패",
    "fallback"
]

STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "into", "over", "under", "amid",
    "news", "said", "says", "will", "after", "about", "more", "than", "their", "its",
    "있다", "했다", "한다", "위해", "대한", "관련", "통해", "이번", "에서", "으로", "까지",
    "및", "등", "것", "수", "더", "한", "해", "고", "를", "이", "은", "는", "가", "도"
}

# ──────────────────────────────────────────
# 유틸 함수
# ──────────────────────────────────────────
def clean_text(text):
    if text is None:
        return ""
    if isinstance(text, float) and pd.isna(text):
        return ""
    text = str(text)
    text = BeautifulSoup(text, "html.parser").get_text(" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_title_key(title):
    title = clean_text(title).lower()
    title = re.sub(r"\[[^\]]+\]", " ", title)
    title = re.sub(r"\([^)]+\)", " ", title)
    title = re.sub(r"\s*[-|]\s*[^-|]+$", "", title)
    title = re.sub(r"[^0-9a-zA-Z가-힣 ]+", " ", title)
    return re.sub(r"\s+", " ", title).strip()


def make_issue_dedup_key(title):
    base = normalize_title_key(title)
    base = re.sub(
        r"\s*-\s*(logistics management|google news global|google news korea|dredgewire)$",
        "",
        base
    )
    tokens = [t for t in base.split() if len(t) >= 2]
    return " ".join(tokens[:6])


def parse_pubdate(date_text):
    try:
        dt = dateparser.parse(str(date_text))
        if dt is None:
            return pd.NaT
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST).replace(tzinfo=None)
    except Exception:
        return pd.NaT


def format_kst(dt):
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).strftime("%Y-%m-%d %H:%M")


def detect_language_safe(text, default="ko"):
    text = clean_text(text)
    if not text or len(text) < 5:
        return default
    try:
        lang = detect(text)
    except Exception:
        lang = default

    if lang.startswith("ko"):
        return "ko"
    if lang.startswith("en"):
        return "en"
    return default


def guess_category(text):
    text_clean = clean_text(text).lower()
    scores = {cat: 0 for cat in CATEGORY_RULES}

    for category, keywords in CATEGORY_RULES.items():
        for kw in keywords:
            kw = kw.lower().strip()
            if re.search(r"[a-z]", kw):
                if re.search(rf"\b{re.escape(kw)}\b", text_clean):
                    scores[category] += 1
            else:
                if kw in text_clean:
                    scores[category] += 1

    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "기타"


def recency_score(dt):
    if pd.isna(dt):
        return 0.5
    now = datetime.now(KST).replace(tzinfo=None)
    hours = (now - pd.Timestamp(dt).to_pydatetime()).total_seconds() / 3600
    hours = max(hours, 0)
    if hours <= 6:
        return 5.0
    if hours <= 12:
        return 4.2
    if hours <= 24:
        return 3.4
    if hours <= 48:
        return 2.3
    if hours <= 72:
        return 1.2
    return 0.5


def description_score(desc):
    length = len(clean_text(desc))
    if length >= 180:
        return 2.5
    if length >= 120:
        return 2.0
    if length >= 60:
        return 1.2
    if length >= 20:
        return 0.7
    return 0.2


def duplicate_score(cluster_count):
    if pd.isna(cluster_count):
        return 0.0
    return min((int(cluster_count) - 1) * 1.2, 4.0)


def score_news_row(row):
    score = 0.0
    score += recency_score(row["발행일"])
    score += description_score(row["기사설명"])
    score += SOURCE_WEIGHT.get(row["출처"], 1.0)
    score += CATEGORY_WEIGHT.get(row["카테고리_규칙"], 1.0)
    score += duplicate_score(row["제목클러스터건수"])
    return round(score, 2)


def clean_user_text(text, fallback=""):
    text = clean_text(text)
    if not text:
        return fallback
    lowered = text.lower()
    if any(bad.lower() in lowered for bad in BAD_UI_PHRASES):
        return fallback
    return text


def safe_int(value, default=0, minimum=None, maximum=None):
    try:
        value = int(value)
    except Exception:
        value = default

    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def normalize_category(value, fallback="기타"):
    value = clean_text(value)
    if value in VALID_CATEGORIES:
        return value
    return fallback if fallback in VALID_CATEGORIES else "기타"


def shorten_text(text, max_len=160):
    text = clean_text(text)
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."


def split_sentences(text):
    text = clean_text(text)
    if not text:
        return []
    parts = re.split(r"(?<=[\.\!\?。！？])\s+|(?<=다\.)\s+", text)
    return [p.strip() for p in parts if p.strip()]


def fallback_keywords(title, desc, top_n=4):
    text = f"{clean_text(title)} {clean_text(desc)}".lower()
    tokens = re.findall(r"[0-9a-zA-Z가-힣]{2,}", text)
    freq = {}

    for token in tokens:
        if token in STOPWORDS:
            continue
        if len(token) < 2:
            continue
        freq[token] = freq.get(token, 0) + 1

    ranked = sorted(freq.items(), key=lambda x: (-x[1], x[0]))
    return [k for k, _ in ranked[:top_n]]


def fallback_importance_reason(category, title, desc):
    category = clean_text(category)
    title_lower = clean_text(title).lower()

    if "정책" in category or "customs" in title_lower or "관세" in title_lower:
        return "정책·제도 변화가 물류 현장과 기업 대응 전략에 직접 영향을 줄 수 있는 이슈입니다."
    if "기술" in category or "ai" in title_lower or "rfid" in title_lower:
        return "기술 도입은 운영 효율과 가시성 개선에 직결되므로 업계 파급력이 큰 편입니다."
    if "항만" in category or "shipping" in title_lower or "port" in title_lower:
        return "항만·해운 운영 변화는 공급망 전반의 흐름과 연결되어 주목할 필요가 있습니다."
    if "리스크" in category:
        return "운영 차질과 비용 변동 가능성을 함께 점검해야 하는 리스크 이슈입니다."
    return "업계 흐름과 실무 대응 방향을 파악하는 데 참고 가치가 높은 이슈입니다."


def fallback_content_angle(category, title, desc):
    category = clean_text(category)
    title_lower = clean_text(title).lower()

    if "정책" in category:
        return "정책 변화 해설, 실무 대응 가이드, 업계 영향 분석 콘텐츠로 활용할 수 있습니다."
    if "기술" in category or "ai" in title_lower:
        return "자동화·디지털 전환 사례와 현장 적용 포인트를 설명하는 콘텐츠에 적합합니다."
    if "항만" in category:
        return "항만 경쟁력과 공급망 연결성 관점에서 풀어내는 해설형 콘텐츠로 활용하기 좋습니다."
    if "투자" in category:
        return "산업 투자 흐름과 기업 전략 변화 관점의 분석형 콘텐츠 소재가 됩니다."
    return "오늘의 물류 브리프, 유튜브 해설, 실무 인사이트 정리 콘텐츠로 확장할 수 있습니다."


def fallback_summary_lines(title, desc, category):
    title = clean_text(title)
    desc = clean_text(desc)
    category = clean_text(category) or "기타"

    sentences = split_sentences(desc)

    if len(sentences) >= 3:
        lines = sentences[:3]
    elif len(sentences) == 2:
        lines = [sentences[0], sentences[1], f"{category} 관점에서 시사점을 점검할 필요가 있습니다."]
    elif len(sentences) == 1:
        lines = [
            title if title else sentences[0],
            sentences[0],
            f"{category} 관점에서 후속 흐름을 살펴볼 필요가 있습니다."
        ]
    else:
        lines = [
            title if title else "주요 물류 이슈입니다.",
            shorten_text(desc, 100) if desc else "핵심 내용 요약이 충분하지 않습니다.",
            f"{category} 관점에서 의미를 살펴볼 필요가 있습니다."
        ]

    cleaned = []
    for line in lines:
        line = clean_user_text(line, "")
        if not line:
            line = "관련 내용을 점검할 필요가 있습니다."
        cleaned.append(shorten_text(line, 110))

    while len(cleaned) < 3:
        cleaned.append(f"{category} 관점에서 추가 확인이 필요합니다.")

    return cleaned[:3]


def normalize_keywords(value, title="", desc=""):
    if isinstance(value, list):
        keywords = [clean_text(v) for v in value if clean_text(v)]
    else:
        value = clean_text(value)
        keywords = [v.strip() for v in value.split(",") if v.strip()] if value else []

    if not keywords:
        keywords = fallback_keywords(title, desc, top_n=4)

    deduped = []
    for kw in keywords:
        if kw not in deduped:
            deduped.append(kw)

    return deduped[:5]


def build_project_overview():
    return {
        "project_name": "국내외 물류 뉴스 자동 브리핑 및 콘텐츠 아이디어 도출 시스템",
        "purpose": "국내외 물류 뉴스를 자동 수집·정리해 대표가 빠르게 핵심 이슈를 파악하고 콘텐츠 기획에 활용할 수 있도록 돕는 시스템",
        "main_features": [
            "RSS 기반 물류 뉴스 자동 수집",
            "중복 제거 및 중요 뉴스 선별",
            "한국어 중심 요약 및 일일 브리프 생성",
            "콘텐츠 아이디어 자동 제안"
        ],
        "final_deliverable": "테오시스 홈페이지용 data.json"
    }

# ──────────────────────────────────────────
# Gemini 호출
# ──────────────────────────────────────────
def extract_json_from_text(raw):
    raw = str(raw).strip()
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    raw = re.sub(r"^```json\s*", "", raw).strip()
    raw = re.sub(r"^```\s*", "", raw).strip()
    raw = re.sub(r"\s*```$", "", raw).strip()

    try:
        return json.loads(raw)
    except Exception:
        pass

    obj_match = re.search(r"\{.*\}", raw, re.DOTALL)
    if obj_match:
        try:
            return json.loads(obj_match.group())
        except Exception:
            pass

    arr_match = re.search(r"\[.*\]", raw, re.DOTALL)
    if arr_match:
        try:
            return json.loads(arr_match.group())
        except Exception:
            pass

    raise ValueError(f"JSON 파싱 실패 | 원문 앞 200자: {raw[:200]}")


def call_gemini_json(prompt, retries=GEMINI_RETRIES, wait=GEMINI_WAIT_SECONDS):
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
            text = resp.text if hasattr(resp, "text") else str(resp)
            return extract_json_from_text(text)
        except Exception as e:
            last_err = e
            err_str = str(e)
            err_lower = err_str.lower()

            # 인증/모델 오류는 재시도 의미가 적음
            if "401" in err_str or "unauthenticated" in err_lower:
                raise RuntimeError(f"Gemini 인증 오류: {err_str}") from e

            if "404" in err_str and "not found" in err_lower:
                raise RuntimeError(f"Gemini 모델/리소스 오류: {err_str}") from e

            # 429는 조금 더 길게 대기
            retry_wait = wait
            matched = re.search(r"retry in (\d+(?:\.\d+)?)s", err_str)
            if matched:
                retry_wait = min(float(matched.group(1)) + 3, 90)
            elif "429" in err_str or "resource_exhausted" in err_lower:
                retry_wait = min(wait * attempt, 90)
            else:
                retry_wait = min(5 * attempt, 30)

            print(f"    ⚠️ Gemini 시도 {attempt}/{retries} 실패 → {retry_wait:.0f}초 대기")
            time.sleep(retry_wait)

    raise RuntimeError(f"Gemini 호출 최종 실패: {last_err}")

# ──────────────────────────────────────────
# 기사 배치 분석 (API 1회 호출로 전체 처리)
# ──────────────────────────────────────────
def analyze_articles_batch(df_candidates):
    articles_text = []
    for i, (_, row) in enumerate(df_candidates.iterrows(), start=1):
        articles_text.append(
            f"[기사{i}]\n"
            f"출처: {row['출처']} / 국내외: {row['국내외구분']} / 언어: {row['언어']}\n"
            f"발행일: {row['발행일_표준']}\n"
            f"제목(원문): {row['기사제목']}\n"
            f"설명(원문): {row['기사설명']}"
        )

    prompt = f"""
너는 물류업 대표에게 아침 뉴스 브리핑을 올리는 수석 비서다.
아래 기사 {len(df_candidates)}개를 분석하여 반드시 JSON 배열로만 출력하라.
설명 문장, 코드블록, 마크다운 없이 JSON 배열만 출력하라.
모든 결과는 한국어 중심으로 작성하되, 번역이 어려우면 원문 핵심 의미를 자연스러운 한국어로 풀어써라.

[공통 출력 규칙]
1) translated_title_ko: 한국어 제목. 실패 시에도 "[번역 대기]" 같은 문구 금지
2) translated_description_ko: 한국어 설명. 과도하게 길지 않게
3) summary_3lines: 반드시 3개 요소의 배열
4) importance_score_ai: 1~10 정수
5) category: 다음 중 하나
   ["항만·해운","항공·운송","창고·물류센터","자동화·기술","정책·공공","안전·리스크","투자·경영","기타"]
6) keywords: 3~5개
7) importance_reason: 경영자 관점에서 왜 중요한지 1문장
8) content_angle: 유튜브/브리핑/인사이트 콘텐츠 관점의 활용 포인트 1문장

[분석할 기사 목록]
{chr(10).join(articles_text)}

[JSON 배열 스키마]
[
  {{
    "idx": 1,
    "translated_title_ko": "한국어 제목",
    "translated_description_ko": "한국어 설명",
    "summary_3lines": ["핵심사실", "배경·맥락", "시사점"],
    "category": "카테고리",
    "importance_score_ai": 7,
    "importance_reason": "왜 중요한지",
    "content_angle": "콘텐츠 활용 포인트",
    "keywords": ["키워드1", "키워드2", "키워드3"]
  }}
]
""".strip()

    try:
        data = call_gemini_json(prompt)
        if not isinstance(data, list):
            raise ValueError("배열 응답이 아님")
        print(f"  ✅ 배치 분석 성공: {len(data)}건 반환")
        return data
    except Exception as e:
        print(f"  ⚠️ 배치 분석 실패: {e}")
        return None


def build_article_analysis(row, item=None):
    title = clean_text(row.get("기사제목", ""))
    desc = clean_text(row.get("기사설명", ""))
    rule_category = normalize_category(row.get("카테고리_규칙", "기타"), "기타")
    language = row.get("언어", "ko")

    fallback_lines = fallback_summary_lines(title, desc, rule_category)
    fallback_reason = fallback_importance_reason(rule_category, title, desc)
    fallback_angle = fallback_content_angle(rule_category, title, desc)
    fallback_kw = fallback_keywords(title, desc, top_n=4)

    if isinstance(item, dict):
        translated_title = clean_user_text(item.get("translated_title_ko", ""), title)
        translated_desc = clean_user_text(item.get("translated_description_ko", ""), desc)

        lines = item.get("summary_3lines", [])
        if not isinstance(lines, list):
            lines = fallback_lines
        lines = [clean_user_text(v, "") for v in lines if clean_user_text(v, "")]
        if len(lines) != 3:
            lines = fallback_lines

        category = normalize_category(item.get("category", ""), rule_category)
        ai_importance = safe_int(item.get("importance_score_ai", 5), default=5, minimum=1, maximum=10)
        importance_reason = clean_user_text(item.get("importance_reason", ""), fallback_reason)
        content_angle = clean_user_text(item.get("content_angle", ""), fallback_angle)
        keywords = normalize_keywords(item.get("keywords", []), title=title, desc=desc)

        return {
            "row_id": row["row_id"],
            "번역제목": translated_title if translated_title else title,
            "번역설명": translated_desc if translated_desc else desc,
            "요약1": lines[0],
            "요약2": lines[1],
            "요약3": lines[2],
            "3줄요약": "\n".join(lines),
            "카테고리": category,
            "AI중요도": ai_importance,
            "왜중요한가": importance_reason,
            "콘텐츠포인트": content_angle,
            "AI핵심키워드": keywords,
            "제목상태": "번역완료" if language == "en" and translated_title != title else ("원문한국어" if language == "ko" else "원문표시"),
            "요약상태": "AI요약",
            "분석상태": "ai"
        }

    # fallback
    return {
        "row_id": row["row_id"],
        "번역제목": title,
        "번역설명": desc,
        "요약1": fallback_lines[0],
        "요약2": fallback_lines[1],
        "요약3": fallback_lines[2],
        "3줄요약": "\n".join(fallback_lines),
        "카테고리": rule_category,
        "AI중요도": 5,
        "왜중요한가": fallback_reason,
        "콘텐츠포인트": fallback_angle,
        "AI핵심키워드": fallback_kw,
        "제목상태": "원문한국어" if language == "ko" else "원문표시",
        "요약상태": "기본정리",
        "분석상태": "fallback"
    }

# ──────────────────────────────────────────
# 일일 브리프 생성
# ──────────────────────────────────────────
def generate_daily_brief(df_top):
    records_text = []
    for _, row in df_top.iterrows():
        records_text.append(
            f"- [{row['국내외구분']}] {row['번역제목']} / 출처:{row['출처']} / 카테고리:{row['카테고리']} / 중요도:{row['중요도점수']} / 요약:{row['3줄요약'].replace(chr(10), ' | ')}"
        )

    prompt = f"""
너는 물류업 대표에게 아침 브리핑을 올리는 비서다.
아래의 '오늘 주요 뉴스'를 바탕으로 읽기 쉬운 일일 브리프를 만들어라.
과장 없이 경영자 관점에서 중요 포인트를 짚어라.
반드시 JSON 객체만 출력하라.

[오늘 주요 뉴스]
{chr(10).join(records_text)}

[출력 규칙]
- 모두 한국어
- 짧지만 핵심이 살아 있어야 함
- 섹션별 2~4문장 수준
- 오늘의한줄은 한 문장

[JSON 스키마]
{{
  "총평": "string",
  "국내동향": "string",
  "해외동향": "string",
  "리스크포인트": "string",
  "기회포인트": "string",
  "오늘의한줄": "string"
}}
""".strip()

    fallback = {
        "총평": "오늘은 국내외 물류 현장에서 정책, 기술, 운영 이슈가 함께 부각된 흐름입니다.",
        "국내동향": "국내 뉴스는 항만 운영, 공공 정책, 현장 운영 효율화 이슈 중심으로 살펴볼 필요가 있습니다.",
        "해외동향": "해외 뉴스는 공급망 기술 혁신과 글로벌 운송 체계 변화가 주요 흐름으로 보입니다.",
        "리스크포인트": "운영 차질과 정책 변화 가능성은 실무 대응 관점에서 계속 점검할 필요가 있습니다.",
        "기회포인트": "자동화·디지털 전환과 운영 혁신 사례는 향후 경쟁력 확보 기회로 이어질 수 있습니다.",
        "오늘의한줄": "오늘은 운영 리스크와 기술 기회를 함께 읽어야 하는 날입니다."
    }

    try:
        data = call_gemini_json(prompt)
        if not isinstance(data, dict):
            return fallback

        normalized = {
            "총평": clean_user_text(data.get("총평", ""), fallback["총평"]),
            "국내동향": clean_user_text(data.get("국내동향", ""), fallback["국내동향"]),
            "해외동향": clean_user_text(data.get("해외동향", ""), fallback["해외동향"]),
            "리스크포인트": clean_user_text(data.get("리스크포인트", ""), fallback["리스크포인트"]),
            "기회포인트": clean_user_text(data.get("기회포인트", ""), fallback["기회포인트"]),
            "오늘의한줄": clean_user_text(data.get("오늘의한줄", ""), fallback["오늘의한줄"]),
        }
        return normalized
    except Exception:
        return fallback

# ──────────────────────────────────────────
# 콘텐츠 아이디어 생성
# ──────────────────────────────────────────
def generate_content_ideas(df_top, idea_count=3):
    records_text = []
    for _, row in df_top.iterrows():
        records_text.append(
            f"- 제목:{row['번역제목']} / 카테고리:{row['카테고리']} / 요약:{row['3줄요약'].replace(chr(10), ' | ')}"
        )

    prompt = f"""
너는 물류 전문 유튜브 채널의 기획자다.
아래 주요 뉴스를 바탕으로 시청자에게 실제로 도움이 될 콘텐츠 아이디어 {idea_count}개를 제안하라.
반드시 JSON만 출력하라.

[주요 뉴스]
{chr(10).join(records_text)}

[출력 규칙]
- 모두 한국어
- format은 설명형 / 해설형 / 비교형 / 전망형 중 하나
- video_title은 실제 유튜브 제목처럼 작성
- based_on_titles는 1~3개 기사 제목
- reason은 실무적/시청자 관점에서 작성

[JSON 스키마]
{{
  "ideas": [
    {{
      "rank": 1,
      "topic": "string",
      "reason": "string",
      "based_on_titles": ["string"],
      "video_title": "string",
      "format": "해설형"
    }}
  ]
}}
""".strip()

    fallback = []
    for i, row in df_top.head(idea_count).reset_index(drop=True).iterrows():
        fallback.append({
            "rank": i + 1,
            "topic": row["카테고리"],
            "reason": clean_user_text(
                row["왜중요한가"],
                "업계 흐름과 실무 대응 관점에서 풀어볼 가치가 있는 이슈입니다."
            ),
            "based_on_titles": [clean_user_text(row["번역제목"], row["기사제목"])],
            "video_title": f"{clean_user_text(row['번역제목'], row['기사제목'])}이 물류업계에 주는 의미",
            "format": "해설형"
        })

    try:
        data = call_gemini_json(prompt)
        if not isinstance(data, dict) or "ideas" not in data or not isinstance(data["ideas"], list):
            return fallback

        normalized = []
        for idx, idea in enumerate(data["ideas"][:idea_count], start=1):
            if not isinstance(idea, dict):
                continue

            based_on_titles = idea.get("based_on_titles", [])
            if not isinstance(based_on_titles, list):
                based_on_titles = []

            fmt = clean_user_text(idea.get("format", ""), "해설형")
            if fmt not in ["설명형", "해설형", "비교형", "전망형"]:
                fmt = "해설형"

            normalized.append({
                "rank": safe_int(idea.get("rank", idx), default=idx, minimum=1),
                "topic": clean_user_text(idea.get("topic", ""), f"물류 인사이트 {idx}"),
                "reason": clean_user_text(
                    idea.get("reason", ""),
                    "시청자에게 업계 변화의 의미를 쉽게 전달할 수 있는 주제입니다."
                ),
                "based_on_titles": [clean_user_text(v, "") for v in based_on_titles if clean_user_text(v, "")][:3],
                "video_title": clean_user_text(idea.get("video_title", ""), f"오늘의 물류 이슈 {idx}"),
                "format": fmt
            })

        if normalized:
            return normalized
        return fallback
    except Exception:
        return fallback

# ──────────────────────────────────────────
# 후보군 선별
# ──────────────────────────────────────────
def select_candidate_pool(df_input, pool_size=10, min_domestic=3, min_global=3):
    domestic = df_input[df_input["국내외구분"] == "국내"].copy()
    global_df = df_input[df_input["국내외구분"] == "해외"].copy()

    selected = pd.concat([
        domestic.head(min_domestic),
        global_df.head(min_global)
    ]).drop_duplicates(subset=["row_id"])

    remain = df_input[~df_input["row_id"].isin(selected["row_id"])].copy()
    need = max(pool_size - len(selected), 0)
    if need > 0:
        selected = pd.concat([selected, remain.head(need)]).drop_duplicates(subset=["row_id"])

    return selected.sort_values(
        ["규칙점수", "발행일"], ascending=[False, False], na_position="last"
    ).reset_index(drop=True)

# ──────────────────────────────────────────
# 최종 선별
# ──────────────────────────────────────────
def select_top_news(df_input, top_n=5, min_domestic=2, min_global=2):
    df_input = df_input.copy()
    df_input["이슈중복키"] = df_input["기사제목"].apply(make_issue_dedup_key)
    df_input = df_input.sort_values(
        ["중요도점수", "발행일"], ascending=[False, False], na_position="last"
    ).copy()

    df_unique = df_input.drop_duplicates(subset=["이슈중복키"], keep="first").copy()

    domestic = df_unique[df_unique["국내외구분"] == "국내"].head(min_domestic)
    global_df = df_unique[df_unique["국내외구분"] == "해외"].head(min_global)
    selected = pd.concat([domestic, global_df]).drop_duplicates(subset=["row_id"])

    remain = df_unique[~df_unique["row_id"].isin(selected["row_id"])].copy()
    need = max(top_n - len(selected), 0)
    if need > 0:
        selected = pd.concat([selected, remain.head(need)]).drop_duplicates(subset=["row_id"])

    selected = selected.sort_values(
        ["중요도점수", "발행일"], ascending=[False, False], na_position="last"
    ).head(top_n).reset_index(drop=True)

    selected["순위"] = selected.index + 1
    return selected

# ──────────────────────────────────────────
# 메인 실행
# ──────────────────────────────────────────
def main():
    print("=" * 50)
    print("물류 뉴스 브리핑 시스템 시작")
    print(f"실행 시각: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST")
    print(f"사용 모델: {GEMINI_MODEL}")
    print("=" * 50)

    # STEP 1. RSS 수집
    records = []
    for feed in RSS_FEEDS:
        try:
            parsed = feedparser.parse(feed["url"])
            entries = parsed.entries[:MAX_ARTICLES_PER_FEED]

            for entry in entries:
                title = clean_text(entry.get("title", ""))
                link = entry.get("link", "")
                pub_raw = entry.get("published", "") or entry.get("pubDate", "") or entry.get("updated", "")
                summary = clean_text(entry.get("summary", "") or entry.get("description", ""))

                records.append({
                    "수집일시": datetime.now(KST).replace(tzinfo=None),
                    "출처": feed["source"],
                    "국내외구분": feed["region"],
                    "기본언어": feed["default_lang"],
                    "기사제목": title,
                    "기사링크": link,
                    "기사설명": summary,
                    "발행일_원문": pub_raw,
                })

            print(f"  ✅ {feed['source']}: {len(entries)}건 수집")
        except Exception as e:
            print(f"  ⚠️ {feed['source']} 수집 실패: {e}")

    df = pd.DataFrame(records)

    if df.empty:
        raise RuntimeError("RSS 수집 결과가 비어 있습니다.")

    print(f"\n[STEP 1] RSS 수집 완료: {len(df)}건")

    # STEP 2. 정제 및 중복 제거
    df["기사제목"] = df["기사제목"].apply(clean_text)
    df["기사설명"] = df["기사설명"].apply(clean_text)
    df["제목정규키"] = df["기사제목"].apply(normalize_title_key)
    df["발행일"] = df["발행일_원문"].apply(parse_pubdate)
    df["발행일_표준"] = df["발행일"].apply(format_kst)

    df["기사설명"] = df.apply(
        lambda r: r["기사설명"] if len(r["기사설명"]) >= 20 else r["기사제목"],
        axis=1
    )

    df["언어"] = df.apply(
        lambda r: detect_language_safe(f"{r['기사제목']} {r['기사설명']}", default=r["기본언어"]),
        axis=1
    )

    # 빈 값 정리
    df = df[
        (df["기사제목"].astype(str).str.len() > 0) &
        (df["기사링크"].astype(str).str.len() > 0)
    ].copy()

    before = len(df)

    # 링크 기준 1차 중복 제거
    df = df.sort_values("발행일", ascending=False, na_position="last").drop_duplicates(
        subset=["기사링크"], keep="first"
    ).copy()

    # 제목 클러스터 계산
    cluster_counts = df["제목정규키"].value_counts().to_dict()
    df["제목클러스터건수"] = df["제목정규키"].map(cluster_counts)

    # 제목 기준 2차 중복 제거
    df = df.drop_duplicates(subset=["제목정규키"], keep="first").copy()

    df["카테고리_규칙"] = df.apply(
        lambda r: guess_category(f"{r['기사제목']} {r['기사설명']}"),
        axis=1
    )

    df = df.reset_index(drop=True)
    df["row_id"] = df.index + 1

    print(f"[STEP 2] 정제 완료: {before}건 → {len(df)}건")

    # STEP 3. 규칙 점수
    df["규칙점수"] = df.apply(score_news_row, axis=1)
    df = df.sort_values(["규칙점수", "발행일"], ascending=[False, False], na_position="last").copy()
    print("[STEP 3] 규칙 기반 점수 계산 완료")

    # STEP 4. 후보군 선별
    df_candidates = select_candidate_pool(
        df,
        pool_size=CANDIDATE_POOL_SIZE,
        min_domestic=CANDIDATE_MIN_DOMESTIC,
        min_global=CANDIDATE_MIN_GLOBAL
    )
    print(f"[STEP 4] 후보군 선별: {len(df_candidates)}건")

    # STEP 5. Gemini 기사 배치 분석
    print(f"[STEP 5] 배치 분석 시작: {len(df_candidates)}건 → Gemini 1회 호출")
    batch_result = analyze_articles_batch(df_candidates)

    result_map = {}
    if isinstance(batch_result, list):
        for pos, item in enumerate(batch_result, start=1):
            if not isinstance(item, dict):
                continue
            idx = safe_int(item.get("idx", pos), default=pos, minimum=1, maximum=len(df_candidates))
            if idx not in result_map:
                result_map[idx] = item

    analysis_results = []
    for i, (_, row) in enumerate(df_candidates.iterrows(), start=1):
        item = result_map.get(i)
        analysis_results.append(build_article_analysis(row.to_dict(), item=item))

    print("[STEP 5] Gemini 분석 완료")

    df_ai = pd.DataFrame(analysis_results)
    df_candidates = df_candidates.merge(df_ai, on="row_id", how="left")

    df_candidates["중요도점수"] = (
        df_candidates["규칙점수"].fillna(0) + df_candidates["AI중요도"].fillna(5) * 1.8
    ).round(2)

    # STEP 6. 최종 선별
    df_top = select_top_news(
        df_candidates,
        top_n=TOP_NEWS_COUNT,
        min_domestic=FINAL_MIN_DOMESTIC,
        min_global=FINAL_MIN_GLOBAL
    )
    print(f"[STEP 6] 최종 뉴스 선별: {len(df_top)}건")

    # STEP 7. 일일 브리프
    brief_data = generate_daily_brief(df_top)
    print("[STEP 7] 일일 브리프 생성 완료")

    # STEP 8. 콘텐츠 아이디어
    ideas = generate_content_ideas(df_top, idea_count=CONTENT_IDEA_COUNT)
    print("[STEP 8] 콘텐츠 아이디어 생성 완료")

    # STEP 9. JSON 출력 조립
    analysis_date = datetime.now(KST).strftime("%Y-%m-%d")
    generated_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    top_news_list = []
    for _, row in df_top.iterrows():
        title = clean_user_text(row["번역제목"], row["기사제목"])
        description = clean_user_text(row["번역설명"], row["기사설명"])

        summary_lines = [
            clean_user_text(row["요약1"], ""),
            clean_user_text(row["요약2"], ""),
            clean_user_text(row["요약3"], "")
        ]

        if len([s for s in summary_lines if s]) != 3:
            summary_lines = fallback_summary_lines(row["기사제목"], row["기사설명"], row["카테고리"])

        keywords = normalize_keywords(
            row["AI핵심키워드"],
            title=row["기사제목"],
            desc=row["기사설명"]
        )

        top_news_list.append({
            "rank": int(row["순위"]),
            "region": row["국내외구분"],
            "classification": row["국내외구분"],
            "source": row["출처"],
            "title": title,
            "display_title": title,
            "original_title": row["기사제목"],
            "summary": summary_lines,
            "summary_text": " ".join(summary_lines),
            "description": description,
            "category": row["카테고리"],
            "importance": int(row["AI중요도"]),
            "reason": clean_user_text(
                row["왜중요한가"],
                fallback_importance_reason(row["카테고리"], row["기사제목"], row["기사설명"])
            ),
            "content_angle": clean_user_text(
                row["콘텐츠포인트"],
                fallback_content_angle(row["카테고리"], row["기사제목"], row["기사설명"])
            ),
            "keywords": keywords,
            "link": row["기사링크"],
            "published": row["발행일_표준"],
            "quality": {
                "title_status": row.get("제목상태", "unknown"),
                "summary_status": row.get("요약상태", "unknown"),
                "analysis_status": row.get("분석상태", "unknown")
            }
        })

    ideas_list = []
    for idx, idea in enumerate(ideas, start=1):
        if not isinstance(idea, dict):
            continue
        ideas_list.append({
            "rank": safe_int(idea.get("rank", idx), default=idx, minimum=1),
            "topic": clean_user_text(idea.get("topic", ""), f"물류 인사이트 {idx}"),
            "reason": clean_user_text(
                idea.get("reason", ""),
                "시청자에게 업계 변화의 의미를 전달할 수 있는 주제입니다."
            ),
            "video_title": clean_user_text(idea.get("video_title", ""), f"오늘의 물류 이슈 {idx}"),
            "format": clean_user_text(idea.get("format", ""), "해설형"),
            "based_on_titles": [
                clean_user_text(v, "") for v in idea.get("based_on_titles", [])
                if clean_user_text(v, "")
            ][:3]
        })

    output = {
        "generated_at": generated_at,
        "analysis_date": analysis_date,
        "project_overview": build_project_overview(),
        "brief": brief_data,
        "top_news": top_news_list,
        "content_ideas": ideas_list
    }

    # STEP 10. 저장
    os.makedirs("docs", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 완료! → {OUTPUT_PATH} 저장됨")
    print(f"   뉴스: {len(top_news_list)}건 / 아이디어: {len(ideas_list)}건")


if __name__ == "__main__":
    main()
