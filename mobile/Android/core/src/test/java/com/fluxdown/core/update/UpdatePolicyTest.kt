package com.fluxdown.core.update

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class UpdatePolicyTest {
    private val available = AppUpdateStatus(phase = UpdatePhase.Available, latestVersion = "2.0.0", hasUpdate = true)

    @Test fun checkThrottleBoundary() {
        val h6 = UpdatePolicy.CHECK_INTERVAL_MS
        assertTrue(UpdatePolicy.shouldCheck(nowMs = 1_000, lastCheckMs = 0, autoCheck = true))
        assertFalse(UpdatePolicy.shouldCheck(nowMs = 10_000 + h6 - 1, lastCheckMs = 10_000, autoCheck = true))
        assertTrue(UpdatePolicy.shouldCheck(nowMs = 10_000 + h6, lastCheckMs = 10_000, autoCheck = true))
        assertFalse(UpdatePolicy.shouldCheck(nowMs = 10_000 + h6, lastCheckMs = 10_000, autoCheck = false))
        assertFalse(UpdatePolicy.shouldCheck(nowMs = 1_000, lastCheckMs = 0, autoCheck = false))
    }

    @Test fun autoDownloadRespectsNetworkAndDecline() {
        assertTrue(UpdatePolicy.shouldAutoDownload(available, unmetered = true, wifiOnly = true, declinedVersion = ""))
        assertFalse(UpdatePolicy.shouldAutoDownload(available, unmetered = false, wifiOnly = true, declinedVersion = ""))
        assertTrue(UpdatePolicy.shouldAutoDownload(available, unmetered = false, wifiOnly = false, declinedVersion = ""))
        assertFalse(UpdatePolicy.shouldAutoDownload(available, unmetered = true, wifiOnly = true, declinedVersion = "2.0.0"))
        assertTrue(UpdatePolicy.shouldAutoDownload(available, unmetered = true, wifiOnly = true, declinedVersion = "1.9.0"))
    }

    @Test fun autoDownloadOnlyForInstallableAvailable() {
        val manual = available.copy(manualReason = UpdateManualReason.ManagedPackage)
        assertFalse(UpdatePolicy.shouldAutoDownload(manual, true, true, ""))
        assertFalse(UpdatePolicy.shouldAutoDownload(available.copy(hasUpdate = false), true, true, ""))
        assertFalse(UpdatePolicy.shouldAutoDownload(available.copy(phase = UpdatePhase.Downloading), true, true, ""))
        assertFalse(UpdatePolicy.shouldAutoDownload(available.copy(phase = UpdatePhase.Ready), true, true, ""))
    }

    @Test fun notifyOncePerVersionAndHonorsSkip() {
        assertTrue(UpdatePolicy.shouldNotify(available, notifiedVersion = "", skippedVersion = ""))
        assertFalse(UpdatePolicy.shouldNotify(available, notifiedVersion = "2.0.0", skippedVersion = ""))
        assertFalse(UpdatePolicy.shouldNotify(available, notifiedVersion = "", skippedVersion = "2.0.0"))
        assertTrue(UpdatePolicy.shouldNotify(available, notifiedVersion = "1.9.0", skippedVersion = "1.8.0"))
        assertFalse(UpdatePolicy.shouldNotify(available.copy(hasUpdate = false), "", ""))
        assertFalse(UpdatePolicy.shouldNotify(available.copy(phase = UpdatePhase.UpToDate), "", ""))
        assertTrue(UpdatePolicy.shouldNotify(available.copy(phase = UpdatePhase.Ready), "", ""))
    }

    @Test fun installerStatusCodesMapToFailures() {
        assertNull(UpdatePolicy.installFailureFor(0)) // SUCCESS
        assertNull(UpdatePolicy.installFailureFor(-1)) // PENDING_USER_ACTION
        assertEquals(UpdateFailure.ElevationCancelled, UpdatePolicy.installFailureFor(3)) // ABORTED
        assertEquals(UpdateFailure.Storage, UpdatePolicy.installFailureFor(6)) // STORAGE
        for (code in listOf(1, 2, 4, 5, 7, 99)) {
            assertEquals("$code", UpdateFailure.Install, UpdatePolicy.installFailureFor(code))
        }
    }

    @Test fun previewBuildsDefaultToFrontier() {
        assertEquals(UpdatePolicy.CHANNEL_FRONTIER, UpdatePolicy.defaultChannel("1.4.0-rc.2"))
        assertEquals(UpdatePolicy.CHANNEL_STABLE, UpdatePolicy.defaultChannel("1.4.0"))
        assertEquals(UpdatePolicy.CHANNEL_STABLE, UpdatePolicy.defaultChannel(""))
    }
}
