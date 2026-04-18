"""
국내외 물류 뉴스 자동 브리핑 및 콘텐츠 아이디어 도출 시스템
GitHub Actions 실행용 스크립트
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

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
if not GEMINI_API_KEY:
    raise EnvironmentError("❌ GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")

client = genai.Client(api_key=GEMINI_API_KEY)
GEMINI_MODEL = "gemini-2.0-flash"

TOP_NEWS_COUNT       = 5
CANDIDATE_POOL_SIZE  = 10
MIN_DOMESTIC         = 3
MIN_GLOBAL           = 3
CONTENT_IDEA_COUNT   = 3
MAX_ARTICLES_PER_FEED = 12

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
    "항만·해운":   ["항만","항구","해운","선사","해상","컨테이너","선박","port","shipping","ocean","vessel","container","maritime"],
    "항공·운송":   ["항공","화물","운송","배송","택배","truck","trucking","delivery","freight","transport","parcel"],
    "창고·물류센터":["창고","물류센터","풀필먼트","센터","warehouse","fulfillment","distribution center","dc"],
    "자동화·기술": ["로봇","자동화","ai","rfid","센서","디지털","플랫폼","robot","automation","technology","tech","software"],
    "정책·공공":   ["정부","정책","법안","장관","세관","공사","공공기관","규제","ministry","government","policy","customs","authority","regulation"],
    "안전·리스크": ["파업","지연","사고","리스크","중단","위험","분실","손실","delay","risk","strike","disruption","accident","loss"],
    "투자·경영":   ["투자","실적","인수","확장","수익","전략","경영","investment","acquisition","earnings","profit","expansion","strategy"]
}

SOURCE_WEIGHT = {
    "물류신문": 2.0,
    "Google News Korea": 1.6,
    "Google News Global": 1.6,
    "Logistics Management": 2.0
}

CATEGORY_WEIGHT = {
    "정책·공공":   2.3,
    "안전·리스크": 2.5,
    "자동화·기술": 2.2,
    "항만·해운":   1.9,
    "항공·운송":   1.9,
    "창고·물류센터":1.8,
    "투자·경영":   1.8,
    "기타":        1.0
}

# ──────────────────────────────────────────
# 유틸 함수
# ──────────────────────────────────────────
def clean_html_text(text):
    if not text:
        return ""
    text = str(text)
    text = BeautifulSoup(text, "html.parser").get_text(" ")
    return re.sub(r"\s+", " ", text).strip()

def normalize_title_key(title):
    title = clean_html_text(title).lower()
    title = re.sub(r"\s*[-|]\s*[^-|]+$", "", title)
    title = re.sub(r"[^0-9a-zA-Z가-힣 ]+", " ", title)
    return re.sub(r"\s+", " ", title).strip()

def make_issue_dedup_key(title):
    base = normalize_title_key(title)
    base = re.sub(r"\s*-\s*(logistics management|google news global|google news korea|dredgewire)$", "", base)
    tokens = [t for t in base.split() if len(t) >= 3]
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
    text = clean_html_text(text)
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
    text_clean = clean_html_text(text).lower()
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
    h = (now - pd.Timestamp(dt).to_pydatetime()).total_seconds() / 3600
    h = max(h, 0)
    if h <= 6:   return 5.0
    if h <= 12:  return 4.2
    if h <= 24:  return 3.4
    if h <= 48:  return 2.3
    if h <= 72:  return 1.2
    return 0.5

def description_score(desc):
    length = len(clean_html_text(desc))
    if length >= 180: return 2.5
    if length >= 120: return 2.0
    if length >= 60:  return 1.2
    if length >= 20:  return 0.7
    return 0.2

def duplicate_score(cluster_count):
    if pd.isna(cluster_count):
        return 0.0
    return min((int(cluster_count) - 1) * 1.2, 4.0)

def score_news_row(row):
    score  = recency_score(row["발행일"])
    score += description_score(row["기사설명"])
    score += SOURCE_WEIGHT.get(row["출처"], 1.0)
    score += CATEGORY_WEIGHT.get(row["카테고리_규칙"], 1.0)
    score += duplicate_score(row["제목클러스터건수"])
    return round(score, 2)

# ──────────────────────────────────────────
# Gemini 호출
# ──────────────────────────────────────────
def extract_json_from_text(raw):
    raw = raw.strip()

    # thinking 태그 제거 (gemini-2.5 계열 대비)
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()

    # 마크다운 코드블록 제거
    raw = re.sub(r"^```json\s*", "", raw).strip()
    raw = re.sub(r"^```\s*",    "", raw).strip()
    raw = re.sub(r"\s*```$",    "", raw).strip()

    # 1차: 전체를 JSON으로 파싱
    try:
        return json.loads(raw)
    except Exception:
        pass

    # 2차: 첫 번째 { ... } 블록 추출
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except Exception:
            pass

    # 3차: [ ... ] 블록 추출
    m = re.search(r"\[.*\]", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except Exception:
            pass

    raise ValueError(f"JSON 파싱 실패 | 원문 앞 200자: {raw[:200]}")

def call_gemini_json(prompt, retries=3, wait=2):
    last_err = None
    for attempt in range(retries):
        try:
            resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
            text = resp.text if hasattr(resp, "text") else str(resp)
            result = extract_json_from_text(text)
            return result
        except Exception as e:
            last_err = e
            print(f"    ⚠️ Gemini 시도 {attempt+1}/{retries} 실패: {e}")
            time.sleep(wait * (attempt + 1))
    raise RuntimeError(f"Gemini 호출 최종 실패: {last_err}")

# ──────────────────────────────────────────
# 기사 분석
# ──────────────────────────────────────────
def fallback_article_analysis(row):
    cat   = row["카테고리_규칙"]
    title = row["기사제목"]
    desc  = row["기사설명"]
    is_en = row["언어"] == "en"
    # 영어 기사는 번역 실패 시 원문 그대로 노출되지 않도록 안내 문구 추가
    title_ko = f"[번역 대기] {title}" if is_en else title
    desc_ko  = f"[번역 대기] {desc[:120]}" if is_en else desc[:120]
    return {
        "translated_title_ko":       title_ko,
        "translated_description_ko": desc_ko,
        "summary_3lines": [
            title_ko,
            f"핵심 내용: {desc_ko}",
            f"시사점: {cat} 관점에서 확인 필요"
        ],
        "category":              cat,
        "importance_score_ai":   5,
        "importance_reason":     "자동 fallback 요약 (AI 분석 재시도 필요)",
        "content_angle":         f"{cat} 이슈를 쉽게 설명하는 해설형 콘텐츠로 연결 가능",
        "keywords":              [cat]
    }

def analyze_article_with_gemini(row):
    is_english = row["언어"] == "en"

    # 영어 기사일 때 번역 지시를 훨씬 강하게 명시
    lang_instruction = """
⚠️ 이 기사는 영어 원문입니다.
- translated_title_ko: 제목을 자연스러운 한국어로 완전히 번역하라. 영어 단어를 섞지 마라.
- translated_description_ko: 설명을 자연스러운 한국어로 완전히 번역하라.
- summary_3lines: 반드시 100% 한국어로만 작성하라. 영어 단어가 하나라도 포함되면 안 된다.
- importance_reason: 반드시 한국어로만 작성하라.
- content_angle: 반드시 한국어로만 작성하라.
- keywords: 한국어 핵심어로 작성하라 (물류 전문 용어는 한국어 표기 사용).
""".strip() if is_english else """
- translated_title_ko: 한국어 제목을 자연스럽게 다듬어 작성하라.
- translated_description_ko: 설명을 한국어로 정리하라.
- 모든 필드를 한국어로 작성하라.
""".strip()

    prompt = f"""
너는 '물류업 대표에게 아침 뉴스 브리핑을 올리는 수석 비서'다.
아래 기사 메타데이터만 보고 과장 없이 핵심만 정리하라.
기사 원문을 상상해서 덧붙이지 말고, 주어진 제목/설명 범위 안에서만 판단하라.
반드시 JSON만 출력하라. 모든 출력은 반드시 한국어로만 작성하라.

[언어 처리 규칙]
{lang_instruction}

[공통 출력 규칙]
1) 모든 필드는 반드시 한국어로만 작성 (영어 혼용 절대 금지)
2) summary_3lines는 정확히 3개의 한국어 문자열
   - 1번째: 핵심 사실 (무슨 일이 있었나)
   - 2번째: 배경·맥락 (왜 중요한가)
   - 3번째: 시사점 (물류업계에 어떤 영향인가)
3) importance_score_ai는 1~10 정수
4) category는 다음 중 하나만 사용:
   ["항만·해운","항공·운송","창고·물류센터","자동화·기술","정책·공공","안전·리스크","투자·경영","기타"]
5) content_angle은 향후 영상/콘텐츠 포인트를 한국어 한 문장으로
6) keywords는 3~5개 한국어 핵심어 리스트

[기사 정보]
출처: {row["출처"]}
국내외구분: {row["국내외구분"]}
언어: {row["언어"]}
발행일: {row["발행일_표준"]}
제목(원문): {row["기사제목"]}
설명(원문): {row["기사설명"]}

[JSON 스키마]
{{
  "translated_title_ko": "한국어 제목",
  "translated_description_ko": "한국어 설명",
  "summary_3lines": ["핵심 사실 (한국어)", "배경·맥락 (한국어)", "시사점 (한국어)"],
  "category": "카테고리",
  "importance_score_ai": 5,
  "importance_reason": "한국어로 작성",
  "content_angle": "한국어로 작성",
  "keywords": ["한국어키워드1", "한국어키워드2", "한국어키워드3"]
}}
""".strip()

    try:
        data = call_gemini_json(prompt)
        if not isinstance(data, dict):
            return fallback_article_analysis(row)
        lines = data.get("summary_3lines", [])
        if not isinstance(lines, list) or len(lines) != 3:
            lines = fallback_article_analysis(row)["summary_3lines"]
        keywords = data.get("keywords", [])
        if not isinstance(keywords, list):
            keywords = []
        return {
            "translated_title_ko":       data.get("translated_title_ko", row["기사제목"]),
            "translated_description_ko": data.get("translated_description_ko", row["기사설명"]),
            "summary_3lines":            lines,
            "category":                  data.get("category", row["카테고리_규칙"]),
            "importance_score_ai":       int(data.get("importance_score_ai", 5)),
            "importance_reason":         data.get("importance_reason", ""),
            "content_angle":             data.get("content_angle", ""),
            "keywords":                  keywords[:5]
        }
    except Exception as e:
        print(f"⚠️ fallback: {row['기사제목'][:40]} / {e}")
        return fallback_article_analysis(row)

# ──────────────────────────────────────────
# 일일 브리프 생성
# ──────────────────────────────────────────
def generate_daily_brief(df_top):
    records_text = []
    for _, row in df_top.iterrows():
        records_text.append(
            f"- [{row['국내외구분']}] {row['번역제목']} / 출처:{row['출처']} / 카테고리:{row['카테고리']} / 중요도:{row['중요도점수']} / 요약:{row['3줄요약'].replace(chr(10),' | ')}"
        )
    prompt = f"""
너는 물류업 대표에게 아침 브리핑을 올리는 비서다.
아래의 '오늘 주요 뉴스'를 바탕으로, 읽기 쉬운 일일 브리프를 만들어라.
과장 없이 경영자 관점에서 중요 포인트를 짚어라.
반드시 JSON만 출력하라.

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

    try:
        return call_gemini_json(prompt)
    except Exception:
        return {
            "총평":      "오늘은 국내외 물류 뉴스가 혼재된 가운데 정책, 기술, 운영 이슈가 함께 나타났습니다.",
            "국내동향":  "국내 뉴스는 정책·항만·운영 이슈 중심으로 점검할 필요가 있습니다.",
            "해외동향":  "해외 뉴스는 자동화, 공급망, 글로벌 운송 흐름의 변화가 눈에 띕니다.",
            "리스크포인트":"지연·규제·운영 차질 관련 뉴스는 실무 대응 관점에서 확인이 필요합니다.",
            "기회포인트": "자동화와 투자 관련 뉴스는 향후 콘텐츠와 사업 전략의 소재가 될 수 있습니다.",
            "오늘의한줄": "오늘은 운영 리스크와 기술 기회를 함께 읽어야 하는 날입니다."
        }

# ──────────────────────────────────────────
# 콘텐츠 아이디어 생성
# ──────────────────────────────────────────
def generate_content_ideas(df_top, idea_count=3):
    records_text = []
    for _, row in df_top.iterrows():
        records_text.append(
            f"- 제목:{row['번역제목']} / 카테고리:{row['카테고리']} / 요약:{row['3줄요약'].replace(chr(10),' | ')}"
        )
    prompt = f"""
너는 물류 전문 유튜브 채널의 기획자다.
아래 주요 뉴스를 바탕으로, 시청자에게 실제로 도움이 될 콘텐츠 아이디어 {idea_count}개를 제안하라.
단순 뉴스 나열이 아니라 '왜 지금 다뤄야 하는지'가 보여야 한다.
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

    try:
        data = call_gemini_json(prompt)
        if isinstance(data, dict) and "ideas" in data:
            return data["ideas"]
    except Exception:
        pass

    # fallback
    fallback = []
    for i, row in df_top.head(idea_count).reset_index(drop=True).iterrows():
        fallback.append({
            "rank": i + 1,
            "topic": row["카테고리"],
            "reason": row["왜중요한가"],
            "based_on_titles": [row["번역제목"]],
            "video_title": f"{row['번역제목']}이 물류업계에 주는 의미",
            "format": "해설형"
        })
    return fallback

# ──────────────────────────────────────────
# 후보군 선별
# ──────────────────────────────────────────
def select_candidate_pool(df_input, pool_size=10, min_domestic=3, min_global=3):
    domestic  = df_input[df_input["국내외구분"] == "국내"].copy()
    global_df = df_input[df_input["국내외구분"] == "해외"].copy()

    selected = pd.concat([
        domestic.head(min_domestic),
        global_df.head(min_global)
    ]).drop_duplicates(subset=["row_id"])

    remain = df_input[~df_input["row_id"].isin(selected["row_id"])].copy()
    need   = max(pool_size - len(selected), 0)
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
    df_input = df_input.sort_values(["중요도점수", "발행일"], ascending=[False, False], na_position="last").copy()
    df_unique = df_input.drop_duplicates(subset=["이슈중복키"], keep="first").copy()

    domestic  = df_unique[df_unique["국내외구분"] == "국내"].head(min_domestic)
    global_df = df_unique[df_unique["국내외구분"] == "해외"].head(min_global)
    selected  = pd.concat([domestic, global_df]).drop_duplicates(subset=["row_id"])

    remain = df_unique[~df_unique["row_id"].isin(selected["row_id"])].copy()
    need   = max(top_n - len(selected), 0)
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
    print("=" * 50)

    # STEP 1. RSS 수집
    records = []
    for feed in RSS_FEEDS:
        try:
            parsed  = feedparser.parse(feed["url"])
            entries = parsed.entries[:MAX_ARTICLES_PER_FEED]
            for entry in entries:
                title   = clean_html_text(entry.get("title", ""))
                link    = entry.get("link", "")
                pub_raw = entry.get("published", "") or entry.get("pubDate", "") or entry.get("updated", "")
                summary = clean_html_text(entry.get("summary", "") or entry.get("description", ""))
                records.append({
                    "수집일시":  datetime.now(KST).replace(tzinfo=None),
                    "출처":      feed["source"],
                    "국내외구분":feed["region"],
                    "기본언어":  feed["default_lang"],
                    "기사제목":  title,
                    "기사링크":  link,
                    "기사설명":  summary,
                    "발행일_원문":pub_raw,
                })
            print(f"  ✅ {feed['source']}: {len(entries)}건 수집")
        except Exception as e:
            print(f"  ⚠️ {feed['source']} 수집 실패: {e}")

    df = pd.DataFrame(records)
    print(f"\n[STEP 1] RSS 수집 완료: {len(df)}건")

    # STEP 2. 정제 및 중복 제거
    df["기사제목"]   = df["기사제목"].apply(clean_html_text)
    df["기사설명"]   = df["기사설명"].apply(clean_html_text)
    df["제목정규키"] = df["기사제목"].apply(normalize_title_key)
    df["발행일"]     = df["발행일_원문"].apply(parse_pubdate)
    df["발행일_표준"]= df["발행일"].apply(format_kst)
    df["언어"] = df.apply(
        lambda r: detect_language_safe(f"{r['기사제목']} {r['기사설명']}", default=r["기본언어"]),
        axis=1
    )
    df["기사설명"] = df.apply(
        lambda r: r["기사설명"] if len(r["기사설명"]) >= 20 else r["기사제목"],
        axis=1
    )

    before = len(df)
    df = df.sort_values("발행일", ascending=False, na_position="last").drop_duplicates(
        subset=["기사링크"], keep="first"
    ).copy()
    cluster_counts = df["제목정규키"].value_counts().to_dict()
    df["제목클러스터건수"] = df["제목정규키"].map(cluster_counts)
    df = df.drop_duplicates(subset=["제목정규키"], keep="first").copy()
    df["카테고리_규칙"] = df.apply(
        lambda r: guess_category(f"{r['기사제목']} {r['기사설명']}"), axis=1
    )
    df = df.reset_index(drop=True)
    df["row_id"] = df.index + 1
    print(f"[STEP 2] 정제 완료: {before}건 → {len(df)}건")

    # STEP 3. 규칙 점수
    df["규칙점수"] = df.apply(score_news_row, axis=1)
    df = df.sort_values(["규칙점수", "발행일"], ascending=[False, False], na_position="last").copy()
    print(f"[STEP 3] 규칙 기반 점수 계산 완료")

    # STEP 4. 후보군 선별
    df_candidates = select_candidate_pool(
        df, pool_size=CANDIDATE_POOL_SIZE, min_domestic=MIN_DOMESTIC, min_global=MIN_GLOBAL
    )
    print(f"[STEP 4] 후보군 선별: {len(df_candidates)}건")

    # STEP 5. Gemini 기사 분석
    analysis_results = []
    for i, (_, row) in enumerate(df_candidates.iterrows(), start=1):
        print(f"  [{i}/{len(df_candidates)}] 분석: {row['기사제목'][:50]}")
        result = analyze_article_with_gemini(row)
        analysis_results.append({
            "row_id":      row["row_id"],
            "번역제목":    result["translated_title_ko"],
            "번역설명":    result["translated_description_ko"],
            "요약1":       result["summary_3lines"][0],
            "요약2":       result["summary_3lines"][1],
            "요약3":       result["summary_3lines"][2],
            "3줄요약":     "\n".join(result["summary_3lines"]),
            "카테고리":    result["category"],
            "AI중요도":    result["importance_score_ai"],
            "왜중요한가":  result["importance_reason"],
            "콘텐츠포인트":result["content_angle"],
            "AI핵심키워드":result["keywords"]
        })
        time.sleep(1.2)

    df_ai = pd.DataFrame(analysis_results)
    df_candidates = df_candidates.merge(df_ai, on="row_id", how="left")
    df_candidates["중요도점수"] = (
        df_candidates["규칙점수"] + df_candidates["AI중요도"] * 1.8
    ).round(2)
    print(f"[STEP 5] Gemini 분석 완료")

    # STEP 6. 최종 선별
    df_top = select_top_news(
        df_candidates, top_n=TOP_NEWS_COUNT, min_domestic=2, min_global=2
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
    generated_at  = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    top_news_list = []
    for _, row in df_top.iterrows():
        top_news_list.append({
            "rank":         int(row["순위"]),
            "region":       row["국내외구분"],
            "source":       row["출처"],
            "title":        row["번역제목"],
            "original_title": row["기사제목"],
            "summary":      [row["요약1"], row["요약2"], row["요약3"]],
            "description":  row["번역설명"],
            "category":     row["카테고리"],
            "importance":   int(row["AI중요도"]),
            "reason":       row["왜중요한가"],
            "content_angle":row["콘텐츠포인트"],
            "keywords":     row["AI핵심키워드"] if isinstance(row["AI핵심키워드"], list) else [],
            "link":         row["기사링크"],
            "published":    row["발행일_표준"]
        })

    ideas_list = []
    for idea in ideas:
        ideas_list.append({
            "rank":            idea.get("rank", 0),
            "topic":           idea.get("topic", ""),
            "reason":          idea.get("reason", ""),
            "video_title":     idea.get("video_title", ""),
            "format":          idea.get("format", ""),
            "based_on_titles": idea.get("based_on_titles", [])
        })

    output = {
        "generated_at": generated_at,
        "analysis_date": analysis_date,
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
