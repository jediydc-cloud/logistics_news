# 📦 물류 뉴스 자동 브리핑 시스템

RSS 수집 → Gemini AI 분석 → GitHub Pages 게시 → 아임웹 위젯 표시

---

## 🗂 파일 구조

```
/
├── main.py                          # 뉴스 수집·분석 메인 스크립트
├── requirements.txt                 # Python 패키지
├── .github/workflows/daily_report.yml  # GitHub Actions 스케줄
└── docs/
    ├── data.json                    # 자동 생성 (GitHub Pages로 서비스)
    └── widget_embed.html            # 아임웹에 붙여 넣을 코드
```

---

## 🚀 설치 가이드

### 1단계 — GitHub 저장소 준비

1. GitHub에서 **새 퍼블릭(Public) 저장소** 생성
2. 이 폴더의 모든 파일을 업로드

   ```bash
   git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
   # 파일 복사 후
   git add .
   git commit -m "Initial commit"
   git push
   ```

---

### 2단계 — GitHub Secrets 설정 (API 키)

1. 저장소 → **Settings → Secrets and variables → Actions**
2. **New repository secret** 클릭
3. 이름: `GEMINI_API_KEY`  
   값: Gemini API 키 붙여 넣기

---

### 3단계 — GitHub Pages 활성화

1. 저장소 → **Settings → Pages**
2. Source: **Deploy from a branch**
3. Branch: `main` / Folder: `/docs`
4. Save 클릭
5. 약 1~2분 후 `https://YOUR_USERNAME.github.io/YOUR_REPO_NAME/` 에서 확인

> `data.json` URL 예시:  
> `https://teaosis.github.io/logistics-news/data.json`

---

### 4단계 — 첫 번째 실행 테스트

GitHub Actions → **Actions 탭** → `Daily Logistics News Report` → **Run workflow** 클릭

성공하면 `docs/data.json` 파일이 생성됩니다.

---

### 5단계 — 아임웹 위젯 삽입

1. `docs/widget_embed.html` 파일 전체 내용 복사
2. 아임웹 편집기 → **HTML 직접입력** 위젯 추가
3. 내용 붙여넣기
4. 파일 상단 `DATA_URL` 값을 본인의 GitHub Pages URL로 수정

   ```js
   const DATA_URL = "https://teaosis.github.io/logistics-news/data.json";
   ```

---

## ⏰ 자동 실행 스케줄

`.github/workflows/daily_report.yml` 설정:

```yaml
schedule:
  - cron: "0 22 * * *"   # 매일 UTC 22:00 = KST 07:00
```

시간을 바꾸려면 cron 값을 수정하세요.  
예) 오전 8시 KST → `"0 23 * * *"`

---

## 🔧 주요 설정값 (main.py)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `TOP_NEWS_COUNT` | 5 | 최종 선별 뉴스 수 |
| `CANDIDATE_POOL_SIZE` | 10 | AI 분석 후보군 크기 |
| `CONTENT_IDEA_COUNT` | 3 | 콘텐츠 아이디어 수 |
| `MAX_ARTICLES_PER_FEED` | 12 | 피드당 최대 수집 건수 |

---

## ❓ 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| Actions 실패 | API 키 미설정 | Secrets에 `GEMINI_API_KEY` 추가 |
| 위젯에 에러 표시 | DATA_URL 오류 | GitHub Pages URL 확인 |
| 뉴스 없음 | RSS 피드 차단 | main.py의 RSS_FEEDS URL 점검 |
| 한국어 깨짐 | 인코딩 오류 | data.json이 UTF-8인지 확인 |
