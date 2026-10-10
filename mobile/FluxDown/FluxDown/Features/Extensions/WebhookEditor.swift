import FluxDomain
import FluxUI
import SwiftUI

/// S12.2 · 端点编辑器（`.sheet`）：compact 为「配置 / 预览」分段，regular 并排双栏。
/// 字段顺序、校验、预设、预览与测试投递语义与 GPUI `webhook_dialog.rs` 逐条对齐；
/// 「发送测试」把当前草稿直接交给 `daemon.webhook.test`，无需先保存。
struct WebhookEditorSheet: View {
    let existing: WebhookEndpoint?
    let model: WebhookModel

    @Environment(HostStore.self) private var store
    @Environment(AppContainer.self) private var container
    @Environment(\.dismiss) private var dismiss
    @Environment(\.horizontalSizeClass) private var sizeClass

    private nonisolated struct HeaderRow: Identifiable, Hashable {
        let id = UUID()
        var key: String
        var value: String
    }

    private nonisolated enum Pane: Hashable { case config, preview }

    @State private var endpointId: String
    @State private var name: String
    @State private var url: String
    @State private var template: String
    @State private var preset: String
    @State private var events: Set<String>
    @State private var queueId: String
    @State private var headers: [HeaderRow]
    @State private var signEnabled: Bool
    @State private var secret: String
    @State private var allowHttp: Bool
    @State private var useProxy: Bool
    @State private var advancedOpen: Bool
    @State private var urlTouched = false
    @State private var showSecret = false
    @State private var secretCopied = false
    @State private var presets: [WebhookPreset] = []
    @State private var variables: [String] = []
    @State private var testing = false
    @State private var testText: (success: Bool, text: String)?
    @State private var saving = false
    @State private var pane: Pane = .config
    @State private var selection: TextSelection?
    @State private var confirmDiscard = false
    /// 打开时的草稿：与之相比判断是否有未保存修改（取消 / 下拉时确认）。
    @State private var baseline: WebhookEndpoint?
    @FocusState private var focus: Field?

    private nonisolated enum Field: Hashable { case name, url, template }

    init(existing: WebhookEndpoint?, model: WebhookModel) {
        self.existing = existing
        self.model = model
        let seed = existing ?? WebhookEndpoint(id: "wh_\(Int(Date().timeIntervalSince1970 * 1000))")
        _endpointId = State(initialValue: seed.id)
        _name = State(initialValue: seed.name)
        _url = State(initialValue: seed.url)
        _template = State(initialValue: seed.bodyTemplate)
        _preset = State(initialValue: seed.preset.isEmpty ? WebhookEndpoint.presetCustom : seed.preset)
        _events = State(initialValue: Set(seed.events))
        _queueId = State(initialValue: seed.queueId)
        let rows = seed.headers.sorted { $0.key < $1.key }.map { HeaderRow(key: $0.key, value: $0.value) }
        _headers = State(initialValue: rows)
        _signEnabled = State(initialValue: !seed.signSecret.isEmpty)
        _secret = State(initialValue: seed.signSecret)
        _allowHttp = State(initialValue: seed.allowHttp)
        _useProxy = State(initialValue: seed.useProxy)
        _advancedOpen = State(initialValue: !rows.isEmpty || !seed.bodyTemplate.isEmpty || !seed.signSecret.isEmpty || seed.allowHttp || seed.useProxy)
    }

    // MARK: 草稿

    /// 草稿 → 模型。
    private func buildDraft() -> WebhookEndpoint {
        var map: [String: String] = [:]
        for row in headers {
            let key = row.key.trimmingCharacters(in: .whitespacesAndNewlines)
            if !key.isEmpty { map[key] = row.value }
        }
        return WebhookEndpoint(
            id: endpointId,
            name: name.trimmingCharacters(in: .whitespacesAndNewlines),
            preset: preset,
            url: url.trimmingCharacters(in: .whitespacesAndNewlines),
            enabled: existing?.enabled ?? true,
            events: WebhookEvent.allCases.map(\.wire).filter { events.contains($0) },
            queueId: queueId,
            headers: map,
            bodyTemplate: template,
            signSecret: signEnabled ? secret.trimmingCharacters(in: .whitespacesAndNewlines) : "",
            allowHttp: allowHttp,
            useProxy: useProxy
        )
    }

    private var isNew: Bool { existing == nil }
    private var urlError: String? { WebhookTemplate.urlErrorKey(url, allowHttp: allowHttp) }
    private var canSave: Bool {
        !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !url.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && urlError == nil
    }

    private var canTest: Bool { !testing && !url.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }

    private var isDirty: Bool {
        guard let baseline else { return false }
        return buildDraft() != baseline
    }

    private var currentPreset: WebhookPreset? { presets.first { $0.id == preset } }

    /// URL 输入下方的提示文案键（ntfy / 钉钉各有专属提示）。
    private static func urlHintKey(for preset: String) -> String {
        switch preset {
        case "ntfy": "webhookUrlHintNtfy"
        case "dingtalk": "webhookUrlHintDingtalk"
        default: "webhookUrlHint"
        }
    }

    private var previewText: String {
        WebhookTemplate.previewRequest(
            url: url,
            firstEvent: buildDraft().firstEventWire,
            signEnabled: signEnabled,
            template: template,
            preset: currentPreset
        )
    }

    // MARK: 视图

    var body: some View {
        NavigationStack {
            Group {
                if sizeClass == .regular {
                    HStack(spacing: 0) {
                        configForm
                        Divider()
                        previewView.frame(maxWidth: 360)
                    }
                } else {
                    switch pane {
                    case .config: configForm
                    case .preview: previewView
                    }
                }
            }
            .navigationTitle(L(isNew ? "webhookDialogAddTitle" : "webhookDialogEditTitle"))
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button(L("cancel")) {
                        if isDirty { confirmDiscard = true } else { dismiss() }
                    }
                    .disabled(saving)
                    .alert(L("mobileWebhookDiscardTitle"), isPresented: $confirmDiscard) {
                        Button(L("mobileWebhookDiscard"), role: .destructive) { dismiss() }
                        Button(L("mobileWebhookKeepEditing"), role: .cancel) {}
                    }
                }
                if sizeClass != .regular {
                    ToolbarItem(placement: .principal) {
                        Picker("", selection: $pane) {
                            Text(L("mobileWebhookPaneConfig")).tag(Pane.config)
                            Text(L("webhookPreviewTitle")).tag(Pane.preview)
                        }
                        .pickerStyle(.segmented)
                        .frame(maxWidth: 260)
                    }
                }
                ToolbarItem(placement: .confirmationAction) {
                    if saving {
                        ProgressView()
                    } else {
                        Button(L("webhookSaveEndpoint")) { Task { await save() } }
                            .fontWeight(.semibold)
                            .disabled(!canSave)
                    }
                }
                ToolbarItemGroup(placement: .bottomBar) {
                    Button {
                        Task { await sendTest() }
                    } label: {
                        if testing {
                            HStack(spacing: 6) {
                                ProgressView().controlSize(.small)
                                Text(L("webhookTesting"))
                            }
                        } else {
                            Text(L("webhookSendTest"))
                        }
                    }
                    .disabled(!canTest)
                    if let testText {
                        Text(testText.text)
                            .font(.footnote)
                            .foregroundStyle(testText.success ? Color.fdStatusSeedingText : Color.fdStatusFailedText)
                            .lineLimit(2)
                    }
                }
                ToolbarItemGroup(placement: .keyboard) {
                    Spacer()
                    Button(L("confirm")) { focus = nil }
                }
            }
        }
        .presentationDetents([.large])
        .interactiveDismissDisabled(isDirty || saving)
        .task {
            if baseline == nil { baseline = buildDraft() }
            if let catalog = await model.loadCatalog() {
                presets = catalog.presets
                variables = catalog.variables
            }
        }
    }

    // MARK: 配置页

    private var configForm: some View {
        Form {
            Section {
                Text(L("webhookDialogDesc")).font(.footnote).foregroundStyle(.secondary)
            }

            if !presets.isEmpty {
                Section {
                    Picker(L("webhookFieldPreset"), selection: $preset) {
                        ForEach(presets) { entry in
                            Text(entry.label).tag(entry.id)
                        }
                    }
                    .pickerStyle(.menu)
                }
            }

            Section {
                TextField(L("webhookFieldName"), text: $name, prompt: Text(currentPreset?.label ?? ""))
                    .focused($focus, equals: .name)
                queuePicker
            }

            Section {
                HStack {
                    TextField(L("webhookFieldUrl"), text: $url, prompt: Text(currentPreset?.urlPlaceholder ?? ""))
                        .font(.fluxMono)
                        .keyboardType(.URL)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .focused($focus, equals: .url)
                        .submitLabel(.done)
                        .onSubmit { urlTouched = true }
                    PasteButton(payloadType: String.self) { strings in
                        if let first = strings.first { url = first.trimmingCharacters(in: .whitespacesAndNewlines); urlTouched = true }
                    }
                    .labelStyle(.iconOnly)
                    .buttonBorderShape(.capsule)
                }
            } header: {
                Text(L("webhookFieldUrl"))
            } footer: {
                VStack(alignment: .leading, spacing: 4) {
                    Text(L(Self.urlHintKey(for: preset)))
                    if urlTouched, let urlError {
                        Label(L(urlError), systemImage: FluxSymbol.failure)
                            .foregroundStyle(Color.fdStatusFailedText)
                    }
                }
            }

            Section {
                LazyVGrid(columns: [GridItem(.adaptive(minimum: 140), spacing: 8)], alignment: .leading, spacing: 8) {
                    ForEach(WebhookEvent.allCases, id: \.wire) { event in
                        eventChip(event)
                    }
                }
                .listRowInsets(EdgeInsets(top: 10, leading: 16, bottom: 10, trailing: 16))
            } header: {
                Text(L("webhookFieldEvents"))
            } footer: {
                VStack(alignment: .leading, spacing: 4) {
                    Text(L("webhookEventsHint"))
                    if events.isEmpty {
                        Label(L("webhookEventsEmpty"), systemImage: FluxSymbol.warning)
                            .foregroundStyle(Color.fdStatusWarningText)
                    }
                }
            }

            Section {
                DisclosureGroup(isExpanded: $advancedOpen) {
                    advancedContent
                } label: {
                    Text(L("webhookAdvanced"))
                }
            }
        }
        .scrollDismissesKeyboard(.interactively)
        .fluxAnimation(.smooth, value: advancedOpen)
        .fluxAnimation(.smooth, value: signEnabled)
        .onChange(of: focus) { old, _ in if old == .url { urlTouched = true } }
    }

    private var queuePicker: some View {
        let queues = store.state.queues
        return Picker(L("webhookFieldQueue"), selection: $queueId) {
            Text(L("webhookQueueAll")).tag("")
            ForEach(queues) { queue in
                Text(queueLabel(queue)).tag(queue.queueId)
            }
            // 端点引用的队列已被删除：仍保留原值，避免编辑时被悄悄改成「全部队列」。
            if !queueId.isEmpty, !queues.contains(where: { $0.queueId == queueId }) {
                Text(queueId).tag(queueId)
            }
        }
        .pickerStyle(.menu)
    }

    private func queueLabel(_ queue: TaskQueue) -> String {
        switch queue.queueId {
        case TaskQueue.main: L("mainQueue")
        case TaskQueue.later: L("laterQueue")
        default: queue.name
        }
    }

    private func eventChip(_ event: WebhookEvent) -> some View {
        let on = events.contains(event.wire)
        return Button {
            if on { events.remove(event.wire) } else { events.insert(event.wire) }
        } label: {
            HStack(spacing: 6) {
                Image(systemName: on ? FluxSymbol.success : "circle")
                Text(L(event.labelKey)).lineLimit(1)
                Spacer(minLength: 0)
            }
            .frame(minHeight: 44)
            .padding(.horizontal, 10)
            .background(on ? Color.accentColor.opacity(0.14) : Color.secondary.opacity(0.10), in: .rect(cornerRadius: 12, style: .continuous))
            .contentShape(.rect)
        }
        .buttonStyle(.plain)
        .accessibilityAddTraits(on ? .isSelected : [])
    }

    // MARK: 高级

    @ViewBuilder
    private var advancedContent: some View {
        // 请求头
        VStack(alignment: .leading, spacing: 8) {
            Text(L("webhookFieldHeaders")).font(.subheadline.weight(.medium))
            ForEach($headers) { $row in
                HStack(spacing: 8) {
                    VStack(alignment: .leading, spacing: 4) {
                        TextField(L("webhookHeaderName"), text: $row.key)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                        TextField(L("webhookHeaderValue"), text: $row.value)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                    }
                    Button {
                        headers.removeAll { $0.id == row.id }
                    } label: {
                        Image(systemName: "minus.circle.fill")
                            .foregroundStyle(Color.fdStatusFailed)
                            .frame(minWidth: 44, minHeight: 44)
                            .contentShape(.rect)
                    }
                    .buttonStyle(.borderless)
                    .accessibilityLabel(L("webhookRowDelete"))
                }
            }
            Button {
                headers.append(HeaderRow(key: "", value: ""))
            } label: {
                Label(L("webhookAddHeader"), systemImage: FluxSymbol.add)
            }
            .buttonStyle(.borderless)
        }

        // 消息模板
        VStack(alignment: .leading, spacing: 8) {
            Text(L("webhookFieldTemplate")).font(.subheadline.weight(.medium))
            ZStack(alignment: .topLeading) {
                TextEditor(text: $template, selection: $selection)
                    .font(.fluxMono)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .focused($focus, equals: .template)
                    .frame(minHeight: 120)
                    .scrollContentBackground(.hidden)
                    .padding(6)
                    .background(.fill.quaternary, in: .rect(cornerRadius: 10, style: .continuous))
                if template.isEmpty {
                    Text(L("webhookTemplatePlaceholder"))
                        .font(.fluxMono)
                        .foregroundStyle(.tertiary)
                        .padding(.horizontal, 12)
                        .padding(.vertical, 14)
                        .allowsHitTesting(false)
                }
            }
            Text(L("webhookTemplateHint")).font(.footnote).foregroundStyle(.secondary)
            if !variables.isEmpty {
                Menu {
                    ForEach(variables, id: \.self) { variable in
                        Button(variable) { insert(variable) }
                    }
                } label: {
                    Label(L("mobileWebhookInsertVariable"), systemImage: "curlybraces")
                }
            }
        }

        // 签名
        Toggle(isOn: Binding(get: { signEnabled }, set: setSign)) {
            SettingsText(title: L("webhookFieldSign"), detail: L("webhookSignDesc"))
        }
        .tint(Color.fdToggleOn)
        if signEnabled {
            HStack(spacing: 8) {
                Group {
                    if showSecret {
                        TextField(L("webhookFieldSign"), text: $secret)
                    } else {
                        SecureField(L("webhookFieldSign"), text: $secret)
                    }
                }
                .font(.fluxMono)
                .textInputAutocapitalization(.never)
                .autocorrectionDisabled()
                Button {
                    showSecret.toggle()
                } label: {
                    Image(systemName: showSecret ? "eye.slash" : "eye")
                        .frame(minWidth: 44, minHeight: 44)
                        .contentShape(.rect)
                }
                .buttonStyle(.borderless)
                .accessibilityLabel(showSecret ? L("webHideKey") : L("webShowKey"))
            }
            Button(L("webhookRegenerate"), systemImage: FluxSymbol.syncing) {
                secretCopied = false
                secret = WebhookTemplate.generateSecret()
            }
            Button(secretCopied ? L("webhookCopied") : L("webhookCopy"), systemImage: secretCopied ? FluxSymbol.done : FluxSymbol.copy) {
                copySecret()
            }
        }

        Toggle(isOn: $allowHttp) {
            SettingsText(title: L("webhookFieldAllowHttp"), detail: L("webhookAllowHttpDesc"))
        }
        .tint(Color.fdToggleOn)
        Toggle(isOn: $useProxy) {
            SettingsText(title: L("webhookFieldUseProxy"), detail: L("webhookUseProxyDesc"))
        }
        .tint(Color.fdToggleOn)
    }

    private func setSign(_ enabled: Bool) {
        signEnabled = enabled
        // 开启签名时给一个够长够随机的起点；用户可随时改成自己的。
        if enabled, secret.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            secret = WebhookTemplate.generateSecret()
        }
    }

    private func copySecret() {
        ExtensionsClipboard.copy(secret)
        secretCopied = true
        Task {
            do {
                try await Task.sleep(for: .seconds(2))
            } catch {
                return
            }
            secretCopied = false
        }
    }

    /// 在光标处插入模板变量；没有选区时追加到末尾。
    private func insert(_ variable: String) {
        if let selection, case let .selection(range) = selection.indices {
            let offset = template.distance(from: template.startIndex, to: range.lowerBound)
            template.replaceSubrange(range, with: variable)
            let caret = template.index(template.startIndex, offsetBy: offset + variable.count)
            self.selection = TextSelection(insertionPoint: caret)
        } else {
            template += variable
        }
        focus = .template
    }

    // MARK: 预览

    private var previewView: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 12) {
                Text(L("webhookPreviewTitle")).font(.headline)
                Text(previewText)
                    .font(.fluxMono)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(12)
                    .background(.fill.quaternary, in: .rect(cornerRadius: 12, style: .continuous))
                Text(L("webhookPreviewMeta"))
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
            .padding()
        }
        .background(Color(uiColor: .systemGroupedBackground))
    }

    // MARK: 动作

    private func save() async {
        guard canSave, !saving else { return }
        saving = true
        let ok = await model.save(buildDraft())
        saving = false
        if ok { dismiss() }
    }

    private func sendTest() async {
        guard canTest else { return }
        testing = true
        testText = nil
        let draft = buildDraft()
        do throws(HostError) {
            let response: WebhookTestResponse = try await container.session.call(HostMethod.daemonWebhookTest, params: draft)
            let report = WebhookTestReport.make(endpointId: draft.id, response: response)
            testText = (report.success, report.text)
        } catch {
            let report = WebhookTestReport.make(endpointId: draft.id, error: error)
            testText = (report.success, report.text)
        }
        testing = false
    }
}
