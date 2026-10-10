import { describe, expect, test } from 'bun:test'
import { formEncode, generateSecret, renderPreview, urlErrorKey, urlHintKey } from './template'

describe('webhook template preview', () => {
  test('placeholders are substituted, unknown segments and stray braces are kept', () => {
    expect(renderPreview('{"text":"{task.fileName} · {task.totalBytesHuman}"}', false)).toBe(
      '{"text":"ubuntu-24.04.2-desktop-amd64.iso · 6.0 GB"}',
    )
    expect(renderPreview('{unknown} {{ {', false)).toBe('{unknown} {{ {')
    expect(renderPreview('q={event.summary}', true)).toBe('q=ubuntu-24.04.2-desktop-amd64.iso+%C2%B7+6.0+GB')
    expect(renderPreview('"{event.title}"', false)).toBe('"Download completed"')
  })

  test('form encoding preserves component-safe characters and encodes separators', () => {
    expect(formEncode("a b-_.!~*'()")).toBe("a+b-_.!~*'()")
    expect(formEncode('/?&=')).toBe('%2F%3F%26%3D')
  })
})

describe('webhook url validation', () => {
  test('empty passes, https passes, http requires the opt-in', () => {
    expect(urlErrorKey('', false)).toBeNull()
    expect(urlErrorKey('https://ntfy.sh/topic', false)).toBeNull()
    expect(urlErrorKey('http://192.168.1.2:8123/api', false)).toBe('webhookUrlWarnHttp')
    expect(urlErrorKey('http://192.168.1.2:8123/api', true)).toBeNull()
  })

  test('other schemes and empty hosts are invalid', () => {
    expect(urlErrorKey('ftp://host', true)).toBe('webhookUrlInvalid')
    expect(urlErrorKey('https:///path', true)).toBe('webhookUrlInvalid')
    expect(urlErrorKey('not a url', true)).toBe('webhookUrlInvalid')
  })
})

describe('webhook secret', () => {
  test('whsec_ + 32 hex, different every time', () => {
    const secret = generateSecret()
    expect(secret.length).toBe(38)
    expect(secret.startsWith('whsec_')).toBeTruthy()
    expect(/^[0-9a-f]{32}$/.test(secret.slice(6))).toBeTruthy()
    expect(generateSecret()).not.toBe(secret)
  })
})

describe('webhook url hint', () => {
  test('ntfy and dingtalk get their own hints, everything else the generic one', () => {
    expect(urlHintKey('ntfy')).toBe('webhookUrlHintNtfy')
    expect(urlHintKey('dingtalk')).toBe('webhookUrlHintDingtalk')
    expect(urlHintKey('wecom')).toBe('webhookUrlHint')
    expect(urlHintKey('custom')).toBe('webhookUrlHint')
  })
})
