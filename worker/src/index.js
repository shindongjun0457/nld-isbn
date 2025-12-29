export default {
  async fetch(request, env, ctx) {
    // --- CORS ---
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    const url = new URL(request.url);

    if (url.pathname !== "/api/isbn") {
      return new Response(JSON.stringify({ ok: false, error: "Not Found" }), {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8", ...corsHeaders() },
      });
    }

    if (request.method !== "POST") {
      return new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
        status: 405,
        headers: { "content-type": "application/json; charset=utf-8", ...corsHeaders() },
      });
    }

    if (!env.NLD_CERT_KEY) {
      return new Response(JSON.stringify({ ok: false, error: "Server missing NLD_CERT_KEY secret" }), {
        status: 500,
        headers: { "content-type": "application/json; charset=utf-8", ...corsHeaders() },
      });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response(JSON.stringify({ ok: false, error: "Invalid JSON body" }), {
        status: 400,
        headers: { "content-type": "application/json; charset=utf-8", ...corsHeaders() },
      });
    }

    const isbns = Array.isArray(body?.isbns) ? body.isbns : null;
    if (!isbns) {
      return new Response(JSON.stringify({ ok: false, error: "Body must be { isbns: string[] }" }), {
        status: 400,
        headers: { "content-type": "application/json; charset=utf-8", ...corsHeaders() },
      });
    }

    // 최대 500개 제한 (프론트도 제한하지만 서버에서도 보강)
    const trimmed = isbns.slice(0, 500);

    // 중복 제거는 하지 않음.
    // 대신 같은 ISBN은 "첫 조회 결과" Promise/결과를 재사용해서 네트워크 호출을 줄임.
    const inBatchCache = new Map(); // normIsbn -> Promise<rowBase>
    const concurrency = clampInt(body?.concurrency, 1, 15, 8);

    const tasks = trimmed.map((rawIsbn, idx) => async () => {
      const norm = normalizeIsbn(rawIsbn);
      // 입력값이 비어있거나 형식이 이상하면 즉시 반환
      if (!norm) {
        return makeRow({
          isbn: String(rawIsbn ?? ""),
          title: "",
          author: "",
          publisher: "",
          year: "",
          found: false,
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
          found: false,
          status: "형식오류",
          note: "ISBN은 숫자 10자리 또는 13자리여야 함",
        });
      }

      // 배치 내 중복: 첫 조회 결과 재사용 (행은 그대로 유지)
      if (!inBatchCache.has(norm)) {
        inBatchCache.set(norm, fetchFromNldOrCache(norm, env, ctx));
      }
      const base = await inBatchCache.get(norm);

      // base는 첫 조회 결과 (성공/미검색/실패)를 담고 있음
      // 중복인 경우 비고만 추가 (첫 등장 index가 아니라면)
      const isDuplicate = firstIndexOfNormalized(trimmed, norm) !== idx;
      if (isDuplicate) {
        return {
          ...base,
          isbn: norm,
          note: base.note ? `${base.note} | 중복: 이전 조회 결과 재사용` : "중복: 이전 조회 결과 재사용",
        };
      }
      return { ...base, isbn: norm };
    });

    const results = await runPool(tasks, concurrency);

    const summary = {
      total: results.length,
      success: results.filter((r) => r["조회결과"] === "성공").length,
      notFound: results.filter((r) => r["조회결과"] === "미검색").length,
      failed: results.filter((r) => r["조회결과"] === "실패").length,
      invalid: results.filter((r) => r["조회결과"] === "형식오류").length,
    };

    return new Response(JSON.stringify({ ok: true, results, summary }), {
      status: 200,
      headers: { "content-type": "application/json; charset=utf-8", ...corsHeaders() },
    });
  },
};

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "POST, OPTIONS",
    "access-control-allow-headers": "content-type",
  };
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

function makeRow({ isbn, title, author, publisher, year, found, status, note }) {
  const authorDisplay = formatAuthorDisplay(author);
  const titleAuthor = title
    ? `${title}(${authorDisplay || "저자미상"})`
    : `(${authorDisplay || "저자미상"})`;

  return {
    "isbn": isbn ?? "",
    "도서명": title ?? "",
    "저자명": author ?? "",
    "도서명(저자명)": title ? `${title}(${authorDisplay || "저자미상"})` : "",
    "출판사": publisher ?? "",
    "발행년도": year ?? "",
    "조회결과": status ?? (found ? "성공" : "미검색"),
    "비고": note ?? "",
  };
}

// 저자 표시 규칙:
// - 여러명일 경우 첫 사람만 + " 외"
// - 단일이면 그대로
function formatAuthorDisplay(authorRaw) {
  const raw = String(authorRaw ?? "").trim();
  if (!raw) return "";

  // 흔한 구분자 기준으로 분해
  const parts = raw.split(/[;|,]/).map((p) => p.trim()).filter(Boolean);

  // 첫 저자명 추출(예: "지은이: 히가시노 게이고" -> "히가시노 게이고")
  const first = (parts[0] ?? raw).split(":").pop().trim();

  const multiple = parts.length >= 2;
  return multiple ? `${first} 외` : first;
}

function extractYearFromDate8(date8) {
  const s = String(date8 ?? "").trim();
  // PUBLISH_PREDATE는 8자리(yyyymmdd)로 안내됨 :contentReference[oaicite:1]{index=1}
  if (/^\d{8}$/.test(s)) return s.slice(0, 4);
  if (/^\d{4}$/.test(s)) return s;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.slice(0, 4);
  return "";
}

async function fetchFromNldOrCache(normIsbn, env, ctx) {
  // Cloudflare cache (영속 캐시: 동일 ISBN 재조회 시 속도/호출량 절감)
  const cacheKey = new Request(`https://cache.local/isbn/${normIsbn}`, { method: "GET" });
  const cached = await caches.default.match(cacheKey);
  if (cached) {
    const data = await cached.json();
    return makeRow(data);
  }

  try {
    const apiUrl = new URL("https://www.nl.go.kr/seoji/SearchApi.do");
    apiUrl.searchParams.set("cert_key", env.NLD_CERT_KEY);
    apiUrl.searchParams.set("result_style", "json");
    apiUrl.searchParams.set("page_no", "1");
    apiUrl.searchParams.set("page_size", "1");
    apiUrl.searchParams.set("isbn", normIsbn);

    const res = await fetch(apiUrl.toString(), {
      headers: { "accept": "application/json" },
    });

    if (!res.ok) {
      const text = await safeText(res);
      const rowBase = {
        isbn: normIsbn,
        title: "",
        author: "",
        publisher: "",
        year: "",
        found: false,
        status: "실패",
        note: `API 응답 오류: HTTP ${res.status}${text ? ` (${text.slice(0, 120)})` : ""}`,
      };
      return makeRow(rowBase);
    }

    const json = await res.json();

    // json 구조: docs 배열(공식 출력 항목에서 docs가 등장) :contentReference[oaicite:2]{index=2}
    const docs = Array.isArray(json?.docs) ? json.docs : [];
    const total = String(json?.TOTAL_COUNT ?? json?.totalCount ?? "").trim();

    if (!docs.length || total === "0") {
      const rowBase = {
        isbn: normIsbn,
        title: "",
        author: "",
        publisher: "",
        year: "",
        found: false,
        status: "미검색",
        note: "",
      };
      // 캐시 저장
      ctx.waitUntil(putCache(cacheKey, rowBase));
      return makeRow(rowBase);
    }

    const d = docs[0] ?? {};
    // 출력 필드: TITLE, AUTHOR, PUBLISHER, PUBLISH_PREDATE 등 :contentReference[oaicite:3]{index=3}
    const title = String(d.TITLE ?? d.title ?? "").trim();
    const author = String(d.AUTHOR ?? d.author ?? "").trim();
    const publisher = String(d.PUBLISHER ?? d.publisher ?? "").trim();

    // 발행년도: PUBLISH_PREDATE(출판예정일) 기준으로 연도 추출.
    // 일부 케이스에서 REAL_PUBLISH_DATE 등이 있을 수 있어 보강.
    const year =
      extractYearFromDate8(d.PUBLISH_PREDATE) ||
      extractYearFromDate8(d.REAL_PUBLISH_DATE) ||
      "";

    const rowBase = {
      isbn: normIsbn,
      title,
      author,
      publisher,
      year,
      found: Boolean(title || author || publisher),
      status: (title || author || publisher) ? "성공" : "미검색",
      note: "",
    };

    // 캐시 저장(예: 30일)
    ctx.waitUntil(putCache(cacheKey, rowBase, 60 * 60 * 24 * 30));
    return makeRow(rowBase);
  } catch (e) {
    const rowBase = {
      isbn: normIsbn,
      title: "",
      author: "",
      publisher: "",
      year: "",
      found: false,
      status: "실패",
      note: `예외: ${String(e?.message ?? e).slice(0, 200)}`,
    };
    return makeRow(rowBase);
  }
}

async function safeText(res) {
  try {
    return await res.text();
  } catch {
    return "";
  }
}

async function putCache(cacheKey, rowBase, maxAgeSeconds = 60 * 60 * 24 * 30) {
  const resp = new Response(JSON.stringify(rowBase), {
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

// 동시성 제한 실행기
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
