package com.fluxdown.bridge

import com.fluxdown.core.host.HostErrorCode
import com.fluxdown.core.host.HostEvent
import com.fluxdown.core.host.HostSignal
import com.fluxdown.core.model.FileExistsAction
import com.fluxdown.core.model.SeedingStatus
import com.fluxdown.core.model.SelectionKind
import com.fluxdown.core.model.SelectionOutcome
import com.fluxdown.core.model.TaskStatus
import com.fluxdown.core.update.AppUpdateSignal
import com.fluxdown.core.update.UpdateFailure
import com.fluxdown.core.update.UpdateManualReason
import com.fluxdown.core.update.UpdatePhase
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class MappingTest {
    @Test
    fun everyErrorCodeMapsToItsNamesake() {
        for (dto in ErrorCodeDto.entries) {
            val core = dto.toCore()
            assertEquals(dto.name.replace("_", "").lowercase(), core.name.lowercase())
        }
    }

    @Test
    fun rpcExceptionKeepsReasonAndRetryable() {
        val host = FluxException.Rpc(ErrorCodeDto.CONFLICT, "revisionConflict", true, "stale revision").toHost()
        assertEquals(HostErrorCode.Conflict, host.code)
        assertEquals("revisionConflict", host.reason)
        assertTrue(host.retryable)
        assertEquals("stale revision", host.message)
    }

    @Test
    fun transportIsRetryableUnavailable() {
        val host = FluxException.Transport("connection refused").toHost()
        assertEquals(HostErrorCode.Unavailable, host.code)
        assertTrue(host.retryable)
        assertNull(host.reason)
        assertEquals("connection refused", host.message)
    }

    @Test
    fun fatalSignalCarriesTheError() {
        val signal = HostSignalDto.Fatal(HostErrorDto(ErrorCodeDto.UNAUTHORIZED, null, false, "bad key")).toCore()
        val error = (signal as HostSignal.Fatal).error
        assertEquals(HostErrorCode.Unauthorized, error.code)
        assertEquals("bad key", error.message)
        assertSame(HostSignal.Stale, HostSignalDto.Stale.toCore())
    }

    @Test
    fun taskDtoMapsStatusSourceBytesAndSeeding() {
        val task = task(status = 5, seeding = 1).toCore()
        assertEquals(TaskStatus.Preparing, task.status)
        assertEquals(SeedingStatus.Seeding, task.seedingStatus)
        assertEquals(6L, task.sourceBytes.total)
        assertEquals(1_700_000_000L, task.createdAt)
        assertEquals(TaskStatus.Unknown, task(status = 99, seeding = 99).toCore().status)
    }

    @Test
    fun unsignedFieldsConvertToLong() {
        val runtime = TaskRuntimeDto("t", ULong.MAX_VALUE.shr(1), 3u, null, 10, emptyList()).toCore()
        assertEquals(Long.MAX_VALUE, runtime.sampleSequence)
        assertEquals(3, runtime.activeTransfers)
        assertNull(runtime.connectedPeers)

        val config = HostEventDto.ConfigChanged(mapOf("k" to "v"), 42uL).toCore() as HostEvent.ConfigChanged
        assertEquals(42L, config.revision)
        assertEquals(mapOf("k" to "v"), config.values)
    }

    @Test
    fun progressEventIsPassedThroughUnchanged() {
        val event = HostEventDto.TaskProgress("t", 4, 1, 2, 3, 4, "f", "deleted", 5, 0).toCore()
        assertEquals(HostEvent.TaskProgress("t", 4, 1, 2, 3, 4, "f", "deleted", 5, 0), event)
    }

    @Test
    fun sectionAndNoticeEventsKeepNameAndJson() {
        assertEquals(
            HostEvent.SectionChanged("agent.gateway", """{"takeoverEnabled":true}"""),
            HostEventDto.SectionChanged("agent.gateway", """{"takeoverEnabled":true}""").toCore(),
        )
        assertEquals(
            HostEvent.Notice("duplicateTorrent", """{"type":"duplicateTorrent"}"""),
            HostEventDto.Notice("duplicateTorrent", """{"type":"duplicateTorrent"}""").toCore(),
        )
    }

    @Test
    fun selectionOutcomesRoundTrip() {
        val outcomes = listOf(
            SelectionOutcome.Hls(2),
            SelectionOutcome.Bt(listOf(0, 3)),
            SelectionOutcome.Variant(1),
            SelectionOutcome.FileExists(FileExistsAction.Rename),
            SelectionOutcome.FileExists(FileExistsAction.Overwrite),
            SelectionOutcome.FileExists(FileExistsAction.Skip),
            SelectionOutcome.Cancelled,
        )
        for (outcome in outcomes) assertEquals(outcome, outcome.toDto().toCore())
    }

    @Test
    fun fileExistsKindKeepsEveryField() {
        val dto = SelectionKindDto.FileExists(
            fileName = "a.bin",
            saveDir = "/data/dl",
            existingSize = 1_048_576uL,
            existingModifiedUnixMs = 1_700_000_000_000L,
            incomingSize = null,
            renamePreview = "a (1).bin",
            actions = listOf(FileExistsActionDto.RENAME, FileExistsActionDto.OVERWRITE),
        )
        assertEquals(
            SelectionKind.FileExists(
                fileName = "a.bin",
                saveDir = "/data/dl",
                existingSize = 1_048_576L,
                existingModifiedUnixMs = 1_700_000_000_000L,
                incomingSize = null,
                renamePreview = "a (1).bin",
                actions = listOf(FileExistsAction.Rename, FileExistsAction.Overwrite),
            ),
            dto.toCore(),
        )
    }

    @Test
    fun updateStatusMapsSizesAndOptionals() {
        val dto = AppUpdateStatusDto(
            phase = UpdatePhaseDto.UP_TO_DATE, currentVersion = "1.0.0", channel = "stable", latestVersion = "1.0.0",
            hasUpdate = false, manualReason = null, assetName = "", assetSize = 5_000_000_000uL, downloadedBytes = 7uL,
            installPending = false, downloadUrl = "", releasePageUrl = "", notes = emptyList(), failure = null,
            errorDetail = "", checkedAtMs = 1_700_000_000_000uL,
        )
        val core = dto.toCore()
        assertEquals(UpdatePhase.UpToDate, core.phase)
        assertEquals(5_000_000_000L, core.assetSize)
        assertEquals(7L, core.downloadedBytes)
        assertEquals(1_700_000_000_000L, core.checkedAtMs)
        assertNull(core.manualReason)
        assertNull(core.failure)
    }

    @Test
    fun updateStatusMapsReasonFailureAndNotes() {
        val dto = AppUpdateStatusDto(
            phase = UpdatePhaseDto.FAILED, currentVersion = "1.0.0", channel = "frontier", latestVersion = "1.1.0",
            hasUpdate = true, manualReason = UpdateManualReasonDto.MANAGED_PACKAGE, assetName = "a.apk", assetSize = 10uL,
            downloadedBytes = 10uL, installPending = true, downloadUrl = "https://d", releasePageUrl = "https://r",
            notes = listOf(AppReleaseNoteDto(version = "1.1.0", publishedAt = "2026-01-01", body = "fix")),
            failure = UpdateFailureDto.ELEVATION_CANCELLED, errorDetail = "x", checkedAtMs = 1uL,
        )
        val core = dto.toCore()
        assertEquals(UpdateManualReason.ManagedPackage, core.manualReason)
        assertEquals(UpdateFailure.ElevationCancelled, core.failure)
        assertEquals(1, core.notes.size)
        assertEquals("fix", core.notes[0].body)
        assertTrue(core.installPending)
    }

    @Test
    fun updateSignalsAndEnumsRoundTrip() {
        val install = AppUpdateSignalDto.InstallRequested("/p/a.apk", "1.1.0").toCore()
        assertEquals(AppUpdateSignal.InstallRequested("/p/a.apk", "1.1.0"), install)
        for (failure in UpdateFailure.entries) assertEquals(failure, failure.toDto().toCore())
        for (reason in UpdateManualReason.entries) assertEquals(reason, reason.toDto().toCore())
        assertEquals(UpdatePhaseDto.entries.size, UpdatePhase.entries.size)
    }

    private fun task(status: Int, seeding: Int) = TaskDto(
        taskId = "t1", url = "https://example.com/a.bin", originUrl = "", fileName = "a.bin", saveDir = "/data",
        status = status, downloadedBytes = 5, totalBytes = 10, errorMessage = "", createdAt = 1_700_000_000,
        completedAt = 0, queueId = "main", groupId = "", rssSourceId = "", fileMissing = false, autoRoute = "",
        sourceCdn = 1, sourceProxy = 2, sourceNic = 3, uploadedBytes = 0, seedingStatus = seeding,
        seedingMessage = "", seedingTimeSecs = 0,
    )
}
