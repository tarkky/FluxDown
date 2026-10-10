package com.fluxdown.core.update

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class UpdateViewTest {
    private fun status(phase: UpdatePhase) = AppUpdateStatus(phase = phase, latestVersion = "2.0.0", hasUpdate = true)

    @Test fun percentIsClampedAndSafeOnUnknownSize() {
        var s = status(UpdatePhase.Downloading)
        assertEquals(0, UpdateView.downloadPercent(s))
        s = s.copy(assetSize = 200, downloadedBytes = 50)
        assertEquals(25, UpdateView.downloadPercent(s))
        s = s.copy(downloadedBytes = 900)
        assertEquals(100, UpdateView.downloadPercent(s))
    }

    @Test fun installOfferedOnlyWhenAutoInstallable() {
        for (phase in listOf(UpdatePhase.Available, UpdatePhase.Ready, UpdatePhase.Failed)) {
            assertTrue("$phase", UpdateView.canInstall(status(phase)))
        }
        val downloading = status(UpdatePhase.Downloading)
        assertTrue(UpdateView.canInstall(downloading))
        assertTrue(UpdateView.canCancel(downloading))
        assertFalse(UpdateView.canInstall(downloading.copy(installPending = true)))
        for (phase in listOf(UpdatePhase.Idle, UpdatePhase.Checking, UpdatePhase.UpToDate, UpdatePhase.Installing)) {
            assertFalse("$phase", UpdateView.canInstall(status(phase)))
        }
        assertFalse(UpdateView.canInstall(status(UpdatePhase.Failed).copy(hasUpdate = false)))
        assertFalse(UpdateView.canInstall(status(UpdatePhase.Available).copy(manualReason = UpdateManualReason.NoAsset)))
    }

    @Test fun manualUrlPrefersAssetLink() {
        var manual = status(UpdatePhase.Available).copy(manualReason = UpdateManualReason.NotWritable, releasePageUrl = "https://r")
        assertEquals("https://r", UpdateView.manualUrl(manual))
        manual = manual.copy(downloadUrl = "https://d")
        assertEquals("https://d", UpdateView.manualUrl(manual))
        assertNull(UpdateView.manualUrl(manual.copy(manualReason = null)))
        assertNull(UpdateView.manualUrl(manual.copy(hasUpdate = false)))
    }

    @Test fun statusLineFollowsPhase() {
        assertNull(UpdateView.statusLine(status(UpdatePhase.Idle)))
        assertEquals(UpdateLine(UpdateText.Checking), UpdateView.statusLine(status(UpdatePhase.Checking)))
        assertEquals(UpdateLine(UpdateText.UpToDate, suffix = " (v2.0.0)"), UpdateView.statusLine(status(UpdatePhase.UpToDate)))
        assertEquals(
            UpdateLine(UpdateText.UpToDate),
            UpdateView.statusLine(AppUpdateStatus(phase = UpdatePhase.UpToDate)),
        )
        assertEquals(UpdateLine(UpdateText.NewVersionFound, version = "2.0.0"), UpdateView.statusLine(status(UpdatePhase.Available)))
        assertNull(UpdateView.statusLine(status(UpdatePhase.Available).copy(hasUpdate = false)))
        assertEquals(
            UpdateLine(UpdateText.Downloading, version = "2.0.0", percent = 50),
            UpdateView.statusLine(status(UpdatePhase.Downloading).copy(assetSize = 10, downloadedBytes = 5)),
        )
        assertEquals(UpdateLine(UpdateText.ReadyToInstall, version = "2.0.0"), UpdateView.statusLine(status(UpdatePhase.Ready)))
        assertEquals(UpdateLine(UpdateText.Installing), UpdateView.statusLine(status(UpdatePhase.Installing)))
    }

    @Test fun failureTextFollowsClassification() {
        assertEquals(UpdateText.FailedUnknown, UpdateView.failureText(null))
        assertEquals(UpdateText.InstallCancelled, UpdateView.failureText(UpdateFailure.ElevationCancelled))
        assertEquals(UpdateText.FailedVerify, UpdateView.failureText(UpdateFailure.Verify, "sha256 mismatch"))
        assertEquals(
            UpdateText.FailedSignature,
            UpdateView.failureText(UpdateFailure.Verify, "${UpdatePolicy.SIGNATURE_MISMATCH}: installed != package"),
        )
        val failed = status(UpdatePhase.Failed).copy(failure = UpdateFailure.Network)
        assertEquals(UpdateLine(UpdateText.FailedNetwork), UpdateView.statusLine(failed))
    }

    @Test fun manualLineOnlyWithNewVersion() {
        val manual = status(UpdatePhase.Available).copy(manualReason = UpdateManualReason.ManagedPackage)
        assertEquals(UpdateLine(UpdateText.ManualStore), UpdateView.manualLine(manual))
        assertNull(UpdateView.manualLine(manual.copy(hasUpdate = false)))
        assertNull(UpdateView.manualLine(status(UpdatePhase.Available)))
        assertEquals(UpdateText.ManualUnsupported, UpdateView.manualReasonText(UpdateManualReason.Unknown))
        assertEquals(UpdateText.ManualUnofficial, UpdateView.manualReasonText(UpdateManualReason.UnofficialBuild))
    }

    @Test fun releaseNotesFollowSystemLanguage() {
        val body = "前言\n<!-- fluxdown:lang:zh -->\n## 问题修复\n- 修复下载\n" +
            "<!-- fluxdown:lang:en -->\n## Bug Fixes\n- Fix downloads\n"
        for (locale in listOf("zh", "zh_CN", "zh-Hans-CN", "zh-Hant-TW", "ZH_hk")) {
            assertEquals(locale, "## 问题修复\n- 修复下载", UpdateView.localizedReleaseBody(body, locale))
        }
        for (locale in listOf("en", "en-US", "fr_FR", "ja", "")) {
            assertEquals(locale, "## Bug Fixes\n- Fix downloads", UpdateView.localizedReleaseBody(body, locale))
        }
    }

    @Test fun releaseNotesHandleMarkerOrderAndWhitespace() {
        val body = "<!--fluxdown:lang:en-->\r\nEnglish\r\n<!--\tfluxdown:lang:zh\n-->\r\n中文\r\n"
        assertEquals("English", UpdateView.localizedReleaseBody(body, "en"))
        assertEquals("中文", UpdateView.localizedReleaseBody(body, "zh"))
    }

    @Test fun releaseNotesPreserveLegacyAndUseAvailableTranslation() {
        val legacy = "## 旧版本\nUntranslated notes\n<!-- unrelated -->"
        assertEquals(legacy, UpdateView.localizedReleaseBody(legacy, "zh"))
        assertEquals(legacy, UpdateView.localizedReleaseBody(legacy, "en"))
        assertEquals("中文", UpdateView.localizedReleaseBody("<!-- fluxdown:lang:zh -->\n中文", "en"))
        assertEquals("English", UpdateView.localizedReleaseBody("<!-- fluxdown:lang:en -->\nEnglish", "zh"))
    }
}
