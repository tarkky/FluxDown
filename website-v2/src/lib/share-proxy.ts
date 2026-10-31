/**
 * FluxMesh分享落地页的 FluxCloud 服务端代理核心：`/s/<slug>` 页面（SSR）与
 * `/api/cloud/share/public/**` 路由共用，浏览器只连本站。
 *
 * 上游契约（FluxCloud，公开、无鉴权、按访客 IP 限频）：
 * - `GET  /api/v1/share/public/{slug}` → 清单 `{ id, slug, title, files[], totalBytes, expiresAt, cloudReady, pickupCode?, turnstileSiteKey? }`
 * - `POST /api/v1/share/public/{slug}/download { fileIndex, turnstileToken? }` → `{ url, expiresAt }`（签名 CDN 链接）
 * - 失败为 HTTP 状态 + `{ code, message }`：404 / 410 `share_ended|share_takedown` / 409 `share_cloud_not_ready` /
 *   403 `traffic_exhausted` / `turnstile_required|turnstile_failed` / 429。
 *
 * 失败原样透传状态码与 `code`（前端按 code 映射文案）；上游不可达 / 未配置 / 返回畸形体统一为 502 + 稳定 code。
 * base 地址与超时约定同 `cloud-proxy.ts`（`FLUXCLOUD_API_BASE`），并附带访客真实 IP（X-Forwarded-For）
 * 以便 FluxCloud 按访客限频。
 */
import { cloudBase } from "./cloud-proxy";
import {
  isRecord,
  isValidSlug,
  parseGrant,
  parseManifest,
  type ShareGrant,
  type ShareLookup,
  SHARE_ROBOTS,
} from "./share-landing";

const UPSTREAM_TIMEOUT_MS = 10_000;

export interface ShareFailure {
  ok: false;
  status: number;
  code: string;
  message?: string;
  retryAfter?: string;
}

export type ShareGrantResult = { ok: true; grant: ShareGrant } | ShareFailure;

export function failure(status: number, code: string, extra: Partial<ShareFailure> = {}): ShareFailure {
  return { ok: false, status, code, ...extra };
}

export async function callUpstream(
  path: string,
  init: { method: string; headers?: Record<string, string>; body?: string },
  ip: string,
  expectBody = true,
): Promise<{ ok: true; body: unknown } | ShareFailure> {
  const base = cloudBase();
  if (!base) return failure(502, "not_configured");

  let upstream: Response;
  try {
    upstream = await fetch(`${base}${path}`, {
      ...init,
      headers: { Accept: "application/json", "X-Forwarded-For": ip, ...init.headers },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
  } catch {
    return failure(502, "upstream_unreachable");
  }

  let body: unknown;
  try {
    body = await upstream.json();
  } catch {
    body = undefined;
  }

  if (!upstream.ok) {
    const code = isRecord(body) && typeof body.code === "string" && body.code !== "" ? body.code : "upstream_error";
    const message = isRecord(body) && typeof body.message === "string" ? body.message : undefined;
    const retryAfter = upstream.headers.get("retry-after") ?? undefined;
    return failure(upstream.status, code, { message, retryAfter });
  }
  if (body === undefined && expectBody) return failure(502, "upstream_invalid");
  return { ok: true, body };
}

/** 拉取分享清单；slug 形状不合法时不打上游，直接按不存在处理。 */
export async function lookupShare(slug: string, ip: string): Promise<ShareLookup> {
  if (!isValidSlug(slug)) return failure(404, "share_not_found");
  const result = await callUpstream(`/api/v1/share/public/${slug}`, { method: "GET" }, ip);
  if (!result.ok) return result;
  const manifest = parseManifest(result.body);
  return manifest ? { ok: true, manifest } : failure(502, "upstream_invalid");
}

/** 申请单个文件的签名下载链接。入参由调用方先经 `parseDownloadRequest` 校验。 */
export async function requestShareDownload(
  slug: string,
  fileIndex: number,
  turnstileToken: string | undefined,
  ip: string,
): Promise<ShareGrantResult> {
  if (!isValidSlug(slug)) return failure(404, "share_not_found");
  const result = await callUpstream(
    `/api/v1/share/public/${slug}/download`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(turnstileToken ? { fileIndex, turnstileToken } : { fileIndex }),
    },
    ip,
  );
  if (!result.ok) return result;
  const grant = parseGrant(result.body);
  return grant ? { ok: true, grant } : failure(502, "upstream_invalid");
}

export interface ShareReport {
  reason: string;
  detail?: string;
  contact?: string;
  turnstileToken?: string;
}

/** 提交举报；上游成功为 204 无正文。入参由调用方先经 `parseReportRequest` 校验。 */
export async function reportShare(slug: string, report: ShareReport, ip: string): Promise<{ ok: true } | ShareFailure> {
  if (!isValidSlug(slug)) return failure(404, "share_not_found");
  const result = await callUpstream(
    `/api/v1/share/public/${slug}/report`,
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(report) },
    ip,
    false,
  );
  return result.ok ? { ok: true } : result;
}

const JSON_HEADERS = {
  "Content-Type": "application/json; charset=utf-8",
  "Cache-Control": "no-store",
  "X-Robots-Tag": SHARE_ROBOTS,
};

export function shareJson(body: unknown, status = 200, extra: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), { status, headers: { ...JSON_HEADERS, ...extra } });
}

/** 成功且无正文（204）。 */
export function shareNoContent(): Response {
  return new Response(null, { status: 204, headers: { "Cache-Control": "no-store", "X-Robots-Tag": SHARE_ROBOTS } });
}

/** 失败 → `{ code, message? }` + 同一状态码（429 保留 Retry-After）。 */
export function failureResponse(fail: ShareFailure): Response {
  const body = fail.message === undefined ? { code: fail.code } : { code: fail.code, message: fail.message };
  return shareJson(body, fail.status, fail.retryAfter ? { "Retry-After": fail.retryAfter } : {});
}
