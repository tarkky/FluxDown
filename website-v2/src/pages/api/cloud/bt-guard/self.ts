/**
 * GET /api/cloud/bt-guard/self — BT 防吸血自查（同源代理）。
 *
 * 转发 FluxCloud `GET /api/v1/bt-guard/public/self`（公开端点，只回答访客来源 IP 所在组，按访客 IP 限频），
 * 校验并裁剪后返回；失败透传状态码与 `{ code, message }`。响应 no-store + `X-Robots-Tag: noindex`。
 * 契约与 IP 透传说明见 `src/lib/bt-guard.ts`、`src/lib/share-proxy.ts`。
 */

import type { APIRoute } from "astro";
import { lookupSelf } from "@/lib/bt-guard-proxy";
import { getClientIp } from "@/lib/client-ip";
import { failureResponse, shareJson } from "@/lib/share-proxy";

export const prerender = false;

export const GET: APIRoute = async ({ request, clientAddress }) => {
  const result = await lookupSelf(getClientIp(request, clientAddress));
  return result.ok ? shareJson(result.self) : failureResponse(result);
};
