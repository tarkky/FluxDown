// Webhook 端点新增 / 编辑对话框。字段顺序、校验、预设、预览与测试投递语义与
// `crates/settings/src/sections/webhook_dialog.rs` 逐条对齐：桌面左表单 + 右实时预览，
// 移动端全屏单栏（预览排在表单下方）。

import { ChevronDown, ChevronRight } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { useT } from '../../i18n'
import { cn } from '../../lib/cn'
import { rpc, useDaemon } from '../../lib/rpc'
import type { QueueDto, WebhookPresetDto } from '../../lib/rpc'
import { Button, CheckRow, Dialog, DialogFooter, FieldHint, FormField, Icon, Input, Select, Textarea, Card } from '../../ui'
import { WEBHOOK_EVENTS, testReport, upsertEndpoint } from './endpoints'
import type { EndpointSpec, TestReport } from './endpoints'
import { EndpointOptions } from './EndpointOptions'
import type { OptionsState } from './EndpointOptions'
import { HeadersEditor } from './HeadersEditor'
import type { HeaderRow } from './HeadersEditor'
import { RequestPreview } from './RequestPreview'
import { DEFAULT_EVENTS, PRESET_CUSTOM, previewRequest, urlErrorKey, urlHintKey } from './template'
import type { RunWrite } from './write'

/** Radix Select 不接受空 value，「全部队列」（queueId=""）用哨兵映射。 */
const ALL_QUEUES = '__all__'
const NO_QUEUES: readonly QueueDto[] = []
const selectQueues = (daemon: { queues: QueueDto[] }) => daemon.queues

export function EndpointDialog({
  existing,
  onClose,
  onSaved,
  runWrite,
}: {
  existing: EndpointSpec | null
  onClose: () => void
  /** 保存成功（参数为端点 id）：调用方据此作废该端点旧的行内测试结果。 */
  onSaved: (endpointId: string) => void
  runWrite: RunWrite
}) {
  const t = useT()
  const queues = useDaemon(selectQueues, NO_QUEUES)

  const newId = useRef(`wh_${Date.now()}`)
  const headerSeq = useRef(0)
  const templateRef = useRef<HTMLTextAreaElement>(null)

  const [name, setName] = useState(existing?.name ?? '')
  const [url, setUrl] = useState(existing?.url ?? '')
  const [template, setTemplate] = useState(existing?.bodyTemplate ?? '')
  const [preset, setPreset] = useState(existing?.preset || PRESET_CUSTOM)
  const [events, setEvents] = useState<ReadonlySet<string>>(() => new Set(existing ? existing.events : DEFAULT_EVENTS))
  const [queueId, setQueueId] = useState(existing?.queueId ?? '')
  const [headers, setHeaders] = useState<HeaderRow[]>(() =>
    Object.entries(existing?.headers ?? {}).map(([key, value]) => ({ id: (headerSeq.current += 1), key, value })),
  )
  const [options, setOptions] = useState<OptionsState>({
    signEnabled: (existing?.signSecret ?? '') !== '',
    secret: existing?.signSecret ?? '',
    allowHttp: existing?.allowHttp ?? false,
    useProxy: existing?.useProxy ?? false,
  })
  // 已有自定义头 / 模板 / 签名的端点直接展开高级区。
  const [advancedOpen, setAdvancedOpen] = useState(
    headers.length > 0 || template !== '' || options.signEnabled || options.allowHttp || options.useProxy,
  )
  // 失焦才亮红字：边打字边报错是噪音，不是帮助。
  const [urlTouched, setUrlTouched] = useState(false)
  const [presets, setPresets] = useState<readonly WebhookPresetDto[]>([])
  const [variables, setVariables] = useState<readonly string[]>([])
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<TestReport | null>(null)
  const [saving, setSaving] = useState(false)

  // 预设目录 + 变量清单来自引擎（`daemon.webhook.get`），前端不复制模板内容。
  useEffect(() => {
    let alive = true
    rpc.daemon.webhook
      .get()
      .then((response) => {
        if (!alive) return
        setPresets(response.presets)
        setVariables(response.variables)
      })
      .catch(() => undefined)
    return () => {
      alive = false
    }
  }, [])

  const currentPreset = presets.find((entry) => entry.id === preset)
  const urlError = urlErrorKey(url, options.allowHttp)
  const canSave = name.trim() !== '' && url.trim() !== '' && urlError === null
  const canTest = !testing && url.trim() !== ''

  /** 草稿 → 模型。 */
  const buildDraft = (): EndpointSpec => {
    const headerMap: Record<string, string> = {}
    for (const row of headers) {
      const key = row.key.trim()
      if (key !== '') headerMap[key] = row.value
    }
    return {
      id: existing?.id ?? newId.current,
      name: name.trim(),
      preset,
      url: url.trim(),
      enabled: existing?.enabled ?? true,
      events: WEBHOOK_EVENTS.map((event) => event.wire).filter((wire) => events.has(wire)),
      queueId,
      headers: headerMap,
      bodyTemplate: template,
      signSecret: options.signEnabled ? options.secret.trim() : '',
      allowHttp: options.allowHttp,
      useProxy: options.useProxy,
    }
  }

  const save = async () => {
    if (!canSave || saving) return
    const draft = buildDraft()
    setSaving(true)
    const ok = await runWrite(() => upsertEndpoint(draft))
    setSaving(false)
    if (ok) {
      onSaved(draft.id)
      onClose()
    }
  }

  /** 页脚「发送测试」：把当前草稿直接交给引擎，无需先保存。 */
  const sendTest = async () => {
    if (!canTest) return
    setTesting(true)
    setTestResult(null)
    try {
      setTestResult(testReport(t, { response: await rpc.daemon.webhook.test({ ...buildDraft() }) }))
    } catch (error) {
      setTestResult(testReport(t, { error }))
    } finally {
      setTesting(false)
    }
  }

  const insertVariable = useCallback(
    (variable: string) => {
      const element = templateRef.current
      const start = element?.selectionStart ?? template.length
      const end = element?.selectionEnd ?? start
      setTemplate(template.slice(0, start) + variable + template.slice(end))
      const caret = start + variable.length
      requestAnimationFrame(() => {
        element?.focus()
        element?.setSelectionRange(caret, caret)
      })
    },
    [template],
  )

  const toggleEvent = (wire: string, checked: boolean) => {
    const next = new Set(events)
    if (checked) next.add(wire)
    else next.delete(wire)
    setEvents(next)
  }

  const queueOptions = [
    { value: ALL_QUEUES, label: t('webhookQueueAll') },
    ...queues.map((queue) => ({
      value: queue.queueId,
      label: queue.queueId === 'main' ? t('mainQueue') : queue.queueId === 'later' ? t('laterQueue') : queue.name,
    })),
  ]
  // 端点引用的队列已被删除：仍保留原值，避免编辑时被悄悄改成「全部队列」。
  if (queueId !== '' && !queues.some((queue) => queue.queueId === queueId)) queueOptions.push({ value: queueId, label: queueId })

  const previewText = previewRequest({
    url,
    firstEvent: WEBHOOK_EVENTS.find((event) => events.has(event.wire))?.wire,
    signEnabled: options.signEnabled,
    template,
    preset: currentPreset,
  })

  const footer = (
    <div className="flex flex-wrap items-center gap-2">
      <div className="flex min-w-0 flex-1 items-center gap-2 mobile:basis-full">
        <Button loading={testing} disabled={!canTest} onClick={() => void sendTest()}>
          {t(testing ? 'webhookTesting' : 'webhookSendTest')}
        </Button>
        {testResult ? (
          <span
            title={testResult.text}
            className={cn('min-w-0 flex-1 truncate text-xs mobile:whitespace-normal', testResult.success ? 'text-success' : 'text-destructive')}
          >
            {testResult.text}
          </span>
        ) : null}
      </div>
      <DialogFooter className="mobile:basis-full">
        <Button onClick={onClose}>{t('cancel')}</Button>
        <Button variant="primary" loading={saving} disabled={!canSave} onClick={() => void save()}>
          {t('webhookSaveEndpoint')}
        </Button>
      </DialogFooter>
    </div>
  )

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !saving) onClose()
      }}
      size="xl"
      className="desktop:w-[min(900px,calc(100vw-32px))]"
      title={t(existing ? 'webhookDialogEditTitle' : 'webhookDialogAddTitle')}
      footer={footer}
    >
      <div className="flex flex-col gap-3">
        <FieldHint>{t('webhookDialogDesc')}</FieldHint>
        {/* 桌面：固定高度双栏（左表单独立滚动、右预览常驻），展开「高级」时对话框不跳动。 */}
        <div className="flex min-h-0 flex-col gap-4 desktop:h-[min(512px,55dvh)] desktop:flex-row desktop:gap-0">
          <div className="flex min-w-0 flex-1 flex-col gap-4 desktop:overflow-y-auto desktop:pr-3">
            {presets.length > 0 ? (
              <FormField label={t('webhookFieldPreset')}>
                <div className="flex flex-wrap gap-1.5">
                  {presets.map((entry) => (
                    <button
                      key={entry.id}
                      type="button"
                      aria-pressed={entry.id === preset}
                      onClick={() => setPreset(entry.id)}
                      className={cn(
                        'inline-flex h-control items-center rounded-md border px-3 text-sm transition-colors coarse:min-h-touch',
                        entry.id === preset
                          ? 'border-primary bg-accent font-medium text-accent-text'
                          : 'border-border text-foreground hover:bg-row-hover',
                      )}
                    >
                      {entry.label}
                    </button>
                  ))}
                </div>
              </FormField>
            ) : null}

            <div className="grid w-full grid-cols-1 gap-3 min-[560px]:grid-cols-2">
              <FormField label={t('webhookFieldName')} htmlFor="webhook-name">
                <Input
                  id="webhook-name"
                  value={name}
                  placeholder={currentPreset?.label ?? ''}
                  onChange={(event) => setName(event.target.value)}
                />
              </FormField>
              <FormField label={t('webhookFieldQueue')} htmlFor="webhook-queue">
                <Select
                  id="webhook-queue"
                  value={queueId === '' ? ALL_QUEUES : queueId}
                  options={queueOptions}
                  onValueChange={(value) => setQueueId(value === ALL_QUEUES ? '' : value)}
                />
              </FormField>
            </div>

            <FormField
              label={t('webhookFieldUrl')}
              htmlFor="webhook-url"
              error={urlTouched && urlError ? t(urlError) : undefined}
              hint={t(urlHintKey(preset))}
            >
              <Input
                id="webhook-url"
                value={url}
                inputMode="url"
                autoCapitalize="off"
                autoCorrect="off"
                spellCheck={false}
                placeholder={currentPreset?.urlPlaceholder ?? ''}
                invalid={urlTouched && urlError !== null}
                onChange={(event) => setUrl(event.target.value)}
                onBlur={() => setUrlTouched(true)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') setUrlTouched(true)
                }}
              />
            </FormField>

            <FormField
              label={t('webhookFieldEvents')}
              error={events.size === 0 ? t('webhookEventsEmpty') : undefined}
              hint={t('webhookEventsHint')}
            >
              <Card className="grid grid-cols-1 gap-x-2 p-1 min-[400px]:grid-cols-2">
                {WEBHOOK_EVENTS.map((event) => (
                  <CheckRow key={event.wire} checked={events.has(event.wire)} onCheckedChange={(checked) => toggleEvent(event.wire, checked)}>
                    {t(event.labelKey)}
                  </CheckRow>
                ))}
              </Card>
            </FormField>

            <button
              type="button"
              aria-expanded={advancedOpen}
              onClick={() => setAdvancedOpen(!advancedOpen)}
              className="flex min-h-control w-full items-center gap-1 text-left text-xs font-medium text-muted-foreground hover:text-foreground coarse:min-h-touch"
            >
              <Icon icon={advancedOpen ? ChevronDown : ChevronRight} size="sm" />
              {t('webhookAdvanced')}
            </button>

            {advancedOpen ? (
              <>
                <HeadersEditor
                  rows={headers}
                  onChange={setHeaders}
                  onAdd={() => setHeaders([...headers, { id: (headerSeq.current += 1), key: '', value: '' }])}
                />
                <div className="flex flex-col gap-2">
                  <FormField label={t('webhookFieldTemplate')} htmlFor="webhook-template" hint={t('webhookTemplateHint')}>
                    <Textarea
                      id="webhook-template"
                      ref={templateRef}
                      rows={5}
                      className="font-mono text-xs"
                      value={template}
                      placeholder={t('webhookTemplatePlaceholder')}
                      autoCapitalize="off"
                      autoCorrect="off"
                      spellCheck={false}
                      onChange={(event) => setTemplate(event.target.value)}
                    />
                  </FormField>
                  <div className="flex flex-wrap gap-1.5">
                    {variables.map((variable) => (
                      <Button key={variable} className="font-mono text-xs" onClick={() => insertVariable(variable)}>
                        {variable}
                      </Button>
                    ))}
                  </div>
                </div>
                <EndpointOptions value={options} onChange={setOptions} />
              </>
            ) : null}
          </div>
          <RequestPreview text={previewText} className="desktop:w-[300px] desktop:shrink-0 desktop:border-l desktop:border-hairline desktop:pl-3" />
        </div>
      </div>
    </Dialog>
  )
}
