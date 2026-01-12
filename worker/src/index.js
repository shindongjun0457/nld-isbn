export default {
  async fetch(request, env, ctx) {
    // --- CORS ---
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    const url = new URL(request.url);

    if (url.pathname !== "/api/isbn") {
      return json({ ok: false, error: "Not Found" }, 404);
    }

    if (request.method !== "POST") {
      return json({ ok: false, error: "Method Not Allowed" }, 405);
    }

    if (!env.NLD_CERT_KEY) {
      return json({ ok: false, error: "Server missing NLD_CERT_KEY secret" }, 500);
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return json({ ok: false, error: "Invalid JSON body" }, 400);
    }

    const isbns = Array.isArray(body?.isbns) ? body.isbns : null;
    if (!isbns) {
      return json({ ok: false, error: "Body must be { isbns: string[] }" }, 400);
    }

    // 최대 500개 제한(서버 보강)
    const input = isbns.slice(0, 500);

    // ✅ 중복 제거하지 않음(행 유지).
    // ✅ 다만 같은 ISBN은 "첫 조회 결과"를 재사용해 외부 API 호출량을 줄임.
    const inBatchCache = new Map(); // normIsbn -> Promise<rowObject>

    // ✅ 동시성 제한: 기본 6 (45개 이상에서도 안정적으로)
    const concurrency = clampInt(body?.concurrency, 1, 10, 6);

    const tasks = input.map((rawIsbn, idx) => async () => {
      const norm = normalizeIsbn(rawIsbn);

      // 형식 검증
      if (!norm) {
        return makeRow({
          isbn: String(rawIsbn ?? ""),
          title: "",
          author: "",
          publisher: "",
          year: "",
          status: "형식오류",
          note: "빈 값 또는 ISBN 형식이 아님",
        });
      }
      if (!(norm.length === 10 || norm.length === 13) || !/^\d+$/.test(norm)) {
        return makeRow({
          isbn: norm,
          title: "",
          author: "",
          publisher: "",
          year: "",
          status: "형식오류",
          note: "ISBN은 숫자 10자리 또는 13자리여야 함",
        });
      }

      // 배치 내 동일 ISBN은 첫 조회 결과 재사용
      if (!inBatchCache.has(norm)) {
        inBatchCache.set(norm, fetchFromNldOrCache(norm, env, ctx));
      }

      const baseRow = await inBatchCache.get(norm);

      // 중복 행은 유지하되, 비고에 재사용 표시(원치 않으면 이 블록 삭제 가능)
      const isDuplicate = firstIndexOfNormalized(input, norm) !== idx;
      if (isDuplicate) {
        const note = baseRow["비고"]
          ? `${baseRow["비고"]} | 중복: 이전 조회 결과 재사용`
          : "중복: 이전 조회 결과 재사용";
        return { ...baseRow, isbn: norm, "비고": note };
      }

      return { ...baseRow, isbn: norm };
    });

    const results = await runPool(tasks, concurrency);

    const summary = {
      total: results.length,
      success: results.filter((r) => r["조회결과"] === "성공").length,
      notFound: results.filter((r) => r["조회결과"] === "미검색").length,
      failed: results.filter((r) => r["조회결과"] === "실패").length,
      invalid: results.filter((r) => r["조회결과"] === "형식오류").length,
    };

    return json({ ok: true, results, summary }, 200);
  },
};

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "POST, OPTIONS",
    "access-control-allow-headers": "content-type",
  };
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...corsHeaders(),
    },
  });
}

function normalizeIsbn(v) {
  const s = String(v ?? "").trim();
  if (!s) return "";
  return s.replace(/[^0-9]/g, ""); // 하이픈/공백 제거
}

function clampInt(v, min, max, def) {
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

// =======================
// AUTHOR NORMALIZATION (START)
// =======================

function escapeRegExp(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isLikelyPersonName(s) {
  if (!s) return false;
  const t = String(s).trim();

  if (t.length < 2 || t.length > 30) return false;
  if (/[0-9]/.test(t)) return false;

  // 역할 단어가 남아있으면 제외(2차 안전장치)
  if (/(지음|글|그림|옮김|번역|역자|감수|편저|엮음|해설|기획)/.test(t)) return false;

  // 한글 이름(공백 포함 가능)
  const koreanOk =
    /^[가-힣\s·.・]+$/.test(t) &&
    t.replace(/\s/g, "").length >= 2 &&
    t.replace(/\s/g, "").length <= 8;

  // 영문 이름
  const englishOk = /^[A-Za-z][A-Za-z .'-]*[A-Za-z]$/.test(t);

  return koreanOk || englishOk;
}

/**
 * authorRaw(문자열) → { authorName, authorShort, count }
 * - authorName: 표의 "저자명" (정리된 이름들, 쉼표)
 * - authorShort: "도서명(저자명)" 괄호용: "첫이름" 또는 "첫이름 외"
 */
function normalizeAuthors(authorRaw) {
  let s = String(authorRaw ?? "").replace(/\u00A0/g, " ").trim();
  if (!s) return { authorName: "", authorShort: "", count: 0 };

  // 괄호/대괄호/중괄호 부가정보 제거
  s = s.replace(/\([^)]*\)/g, " ");
  s = s.replace(/\[[^\]]*\]/g, " ");
  s = s.replace(/\{[^}]*\}/g, " ");

  // 구분자 통일
  s = s
    .replace(/[\u00B7·•]/g, ",")
    .replace(/[;|/]/g, ",")
    .replace(/\s+(and|AND|And)\s+/g, ",")
    .replace(/\s*&\s*/g, ",");

  // 역할/행위 표기 제거
  const roleWords = [
    "지음","씀","저","저자","저술","글","글쓴이","원저","원작","저작",
    "그림","삽화","사진","만화","일러스트","일러스트레이션","그린이",
    "옮김","옮긴이","번역","역","역자","편역",
    "감수","감역","감독",
    "편","편집","편저","엮음","엮은이","편찬",
    "해설","기획","구성","감수자","역해"
  ];
  const rolePattern = new RegExp(`\\s*(?:${roleWords.map(escapeRegExp).join("|")})\\s*`, "g");
  s = s.replace(rolePattern, " ");

  // "지은이: 홍길동" 형태 등 콜론 앞 역할 제거
  s = s.replace(/(지은이|저자|글쓴이)\s*:\s*/g, "");

  s = s.replace(/\s+/g, " ").trim();

  // 후보 분리
  let parts = s.split(",").map(x => x.trim()).filter(Boolean);

  // 접두/접미 정리
  parts = parts.map(p => {
    p = p.replace(/^(저자|저|지은이|글쓴이|저술자)\s*/g, "");
    p = p.replace(/\s*(외|등)$/g, "");
    return p.trim();
  }).filter(Boolean);

  const people = parts.filter(isLikelyPersonName);
  if (people.length === 0) return { authorName: "", authorShort: "", count: 0 };

  const first = people[0];
  return {
    authorName: people.join(", "),
    authorShort: people.length >= 2 ? `${first} 외` : first,
    count: people.length
  };
}

// =======================
// AUTHOR NORMALIZATION (END)
// =======================


function makeRow({ isbn, title, author, publisher, year, status, note }) {
  const normA = normalizeAuthors(author);

// "저자명"은 정규화된 이름들(가능하면 깔끔하게)
const authorName = normA.authorName;

// "도서명(저자명)" 괄호 안은 반드시 "사람이름" 또는 "사람이름 외"만
// 저자가 없으면 괄호 자체를 생략(원치 않으면 "저자미상"으로 바꿀 수 있음)
const titleAuthor = title
  ? (normA.authorShort ? `${title}(${normA.authorShort})` : title)
  : "";


  return {
    isbn: isbn ?? "",
    "도서명": title ?? "",
    "저자명": authorName ?? "",
    "도서명(저자명)": titleAuthor,
    "출판사": publisher ?? "",
    "발행년도": year ?? "",
    "조회결과": status ?? "미검색",
    "비고": note ?? "",
  };
}

// 저자 표시 규칙:
// - 여러명일 경우 첫 사람만 + " 외"
// - 단일이면 그대로
function formatAuthorDisplay(authorRaw) {
  const raw = String(authorRaw ?? "").trim();
  if (!raw) return "";

  // 흔한 구분자 기준
  const parts = raw.split(/[;|,]/).map((p) => p.trim()).filter(Boolean);

  // "지은이: ..." 같은 형태 보정
  const first = (parts[0] ?? raw).split(":").pop().trim();

  return parts.length >= 2 ? `${first} 외` : first;
}

function extractYear(dateLike) {
  const s = String(dateLike ?? "").trim();
  if (/^\d{8}$/.test(s)) return s.slice(0, 4);         // yyyymmdd
  if (/^\d{4}$/.test(s)) return s;                     // yyyy
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.slice(0, 4); // yyyy-mm-dd
  return "";
}

async function fetchFromNldOrCache(normIsbn, env, ctx) {
  // Cloudflare cache (동일 ISBN 재조회 최적화)
  const cacheKey = new Request(`https://cache.local/isbn/${normIsbn}`, { method: "GET" });
  const cached = await caches.default.match(cacheKey);
  if (cached) {
    const data = await cached.json();
    return makeRow(data);
  }

  const apiUrl = new URL("https://www.nl.go.kr/seoji/SearchApi.do");
  apiUrl.searchParams.set("cert_key", env.NLD_CERT_KEY);
  apiUrl.searchParams.set("result_style", "json");
  apiUrl.searchParams.set("page_no", "1");
  apiUrl.searchParams.set("page_size", "1");
  apiUrl.searchParams.set("isbn", normIsbn);

  let res;
try {
  res = await fetchWithRetry(
    apiUrl.toString(),
    { headers: { accept: "application/json" } },
    3500,   // 타임아웃 3.5초 (권장)
    2       // 재시도 2회 (총 3번 시도)
  );
} catch (e) {
  const base = {
    isbn: normIsbn,
    title: "",
    author: "",
    publisher: "",
    year: "",
    status: "실패",
    note: `API fetch 실패(타임아웃/연결): ${String(e?.message ?? e).slice(0, 160)}`,
  };
  return makeRow(base);
}

if (!res.ok) {
  const text = await safeText(res);
  const base = {
    isbn: normIsbn,
    title: "",
    author: "",
    publisher: "",
    year: "",
    status: "실패",
    note: `API 응답 오류: HTTP ${res.status}${text ? ` (${text.slice(0, 120)})` : ""}`,
  };
  return makeRow(base);
}

  let jsonData;
  try {
    jsonData = await res.json();
  } catch (e) {
    const base = {
      isbn: normIsbn,
      title: "",
      author: "",
      publisher: "",
      year: "",
      status: "실패",
      note: "API JSON 파싱 실패",
    };
    return makeRow(base);
  }

  const docs = Array.isArray(jsonData?.docs) ? jsonData.docs : [];
  const total = String(jsonData?.TOTAL_COUNT ?? jsonData?.totalCount ?? "").trim();

  if (!docs.length || total === "0") {
    const base = {
      isbn: normIsbn,
      title: "",
      author: "",
      publisher: "",
      year: "",
      status: "미검색",
      note: "",
    };
    ctx.waitUntil(putCache(cacheKey, base));
    return makeRow(base);
  }

  const d = docs[0] ?? {};
  const title = String(d.TITLE ?? d.title ?? "").trim();
  const author = String(d.AUTHOR ?? d.author ?? "").trim();
  const publisher = String(d.PUBLISHER ?? d.publisher ?? "").trim();

  // 발행년도: PUBLISH_PREDATE 우선, 없으면 대체 필드 보강
  const year =
    extractYear(d.PUBLISH_PREDATE) ||
    extractYear(d.REAL_PUBLISH_DATE) ||
    extractYear(d.PUBLISH_DATE) ||
    "";

  const ok = Boolean(title || author || publisher);

  const base = {
    isbn: normIsbn,
    title,
    author,
    publisher,
    year,
    status: ok ? "성공" : "미검색",
    note: "",
  };

  ctx.waitUntil(putCache(cacheKey, base, 60 * 60 * 24 * 30)); // 30일
  return makeRow(base);
}

async function fetchWithRetry(url, options, timeoutMs, retryCount = 1) {
  // 1회 + retryCount회(예: 1) = 최대 2회 시도
  let lastErr;
  for (let attempt = 0; attempt <= retryCount; attempt++) {
    try {
      const res = await fetchWithTimeout(url, options, timeoutMs);
      return res;
    } catch (e) {
      lastErr = e;
      // 짧은 backoff (200ms, 400ms ...)
      await sleep(200 * Math.pow(2, attempt));
    }
  }
  // 여기까지 왔다는 건 fetch 자체가 실패(연결/타임아웃)
  throw lastErr;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 5000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(id);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function safeText(res) {
  try {
    return await res.text();
  } catch {
    return "";
  }
}

async function putCache(cacheKey, base, maxAgeSeconds = 60 * 60 * 24 * 30) {
  const resp = new Response(JSON.stringify(base), {
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": `public, max-age=${maxAgeSeconds}`,
    },
  });
  await caches.default.put(cacheKey, resp);
}

function firstIndexOfNormalized(list, normIsbn) {
  for (let i = 0; i < list.length; i++) {
    if (normalizeIsbn(list[i]) === normIsbn) return i;
  }
  return -1;
}

// ✅ 동시성 제한 실행기
async function runPool(taskFns, limit) {
  const results = new Array(taskFns.length);
  let i = 0;

  const workers = new Array(Math.min(limit, taskFns.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= taskFns.length) break;
      results[idx] = await taskFns[idx]();
    }
  });

  await Promise.all(workers);
  return results;
}
