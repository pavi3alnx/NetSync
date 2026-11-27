package com.netsync.gui;

import com.netsync.common.ProtocolLimits;
import com.netsync.transfer.TransferChecksum;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic headless tests for {@link TransferPresentation} (labels, Save As gating, and countdown). */
class TransferPresentationTest {

    @Test
    void distinguishesOutgoingCompletedSourceLabelFromIncomingSavedLabel() {
        UUID tid = UUID.randomUUID();
        UUID peer = UUID.randomUUID();
        Path sourcePath = Path.of("sample-source.png").toAbsolutePath();
        Path receivedPath = Path.of("data", "uploads", "clients", peer.toString(), tid + ".bin").toAbsolutePath();

        ClientModels.TransferItem outgoingCompleted = new ClientModels.TransferItem(
                tid, tid, ClientModels.TransferDirection.OUTGOING, peer, "bob", "Screenshot (1).png",
                sourcePath, 1024, 1024, ClientModels.TransferStatus.COMPLETED,
                "Recipient confirmed successful delivery and SHA-256 verification",
                TransferChecksum.EMPTY_SHA256, true, 0L);

        assertEquals(Optional.of("Source file: " + sourcePath), TransferPresentation.completedFilePathLabel(outgoingCompleted));
        assertEquals("Open Source Folder", TransferPresentation.openFolderButtonText(outgoingCompleted));
        assertFalse(TransferPresentation.canSaveAs(outgoingCompleted), "Sender must never have receiver Save As enabled");
        assertTrue(TransferPresentation.shouldShowSha256Verified(outgoingCompleted));

        ClientModels.TransferItem incomingCompleted = new ClientModels.TransferItem(
                tid, tid, ClientModels.TransferDirection.INCOMING, peer, "eve", "Screenshot (1).png",
                receivedPath, 1024, 1024, ClientModels.TransferStatus.COMPLETED,
                "Saved and SHA-256 verified",
                TransferChecksum.EMPTY_SHA256, true, 0L);

        assertEquals(Optional.of("Saved to: " + receivedPath), TransferPresentation.completedFilePathLabel(incomingCompleted));
        assertEquals("Open Folder", TransferPresentation.openFolderButtonText(incomingCompleted));
        assertTrue(TransferPresentation.canSaveAs(incomingCompleted));
        assertTrue(TransferPresentation.shouldShowSha256Verified(incomingCompleted));
    }

    @Test
    void nonCompletedOrUnverifiedTransfersNeverExposeCompletionLabelsOrSaveAs() {
        UUID tid = UUID.randomUUID();
        UUID peer = UUID.randomUUID();
        Path path = Path.of("received.bin").toAbsolutePath();

        ClientModels.TransferStatus[] nonCompleted = {
                ClientModels.TransferStatus.OFFERING,
                ClientModels.TransferStatus.OFFERED,
                ClientModels.TransferStatus.TRANSFERRING,
                ClientModels.TransferStatus.REJECTED,
                ClientModels.TransferStatus.CANCELLED,
                ClientModels.TransferStatus.FAILED
        };
        for (ClientModels.TransferStatus status : nonCompleted) {
            ClientModels.TransferItem item = new ClientModels.TransferItem(
                    tid, tid, ClientModels.TransferDirection.INCOMING, peer, "eve", "file.png",
                    path, 100, 100, status, "status", TransferChecksum.EMPTY_SHA256, true, 1_000L);
            assertTrue(TransferPresentation.completedFilePathLabel(item).isEmpty(), "No path label for " + status);
            assertFalse(TransferPresentation.shouldShowSha256Verified(item), "No SHA-256 verified badge for " + status);
            assertFalse(TransferPresentation.canSaveAs(item), "No Save As for " + status);
        }

        // Completed incoming transfer without verified SHA-256 must not enable Save As or SHA-256 verified badge.
        ClientModels.TransferItem unverified = new ClientModels.TransferItem(
                tid, tid, ClientModels.TransferDirection.INCOMING, peer, "eve", "file.png",
                path, 100, 100, ClientModels.TransferStatus.COMPLETED, "Saved", null, false, 0L);
        assertFalse(TransferPresentation.shouldShowSha256Verified(unverified));
        assertFalse(TransferPresentation.canSaveAs(unverified));
    }

    @Test
    void incomingOfferCountdownTracksAuthoritative30SecondTimeoutMonotonically() {
        assertEquals(30, ProtocolLimits.TRANSFER_TIMEOUT_SECONDS);
        UUID tid = UUID.randomUUID();
        UUID peer = UUID.randomUUID();
        long offerStart = 10_000_000_000L;

        ClientModels.TransferItem offered = new ClientModels.TransferItem(
                tid, tid, ClientModels.TransferDirection.INCOMING, peer, "eve", "Screenshot (1).png",
                null, 0, 2048, ClientModels.TransferStatus.OFFERED, "Incoming offer", null, false, offerStart);

        assertEquals(Optional.of(30L), TransferPresentation.estimatedRemainingOfferSeconds(offered, offerStart));
        assertEquals(Optional.of("Respond within approximately 30 seconds"),
                TransferPresentation.countdownLabel(offered, offerStart));

        long after6s = offerStart + TimeUnit.SECONDS.toNanos(6);
        assertEquals(Optional.of(24L), TransferPresentation.estimatedRemainingOfferSeconds(offered, after6s));
        assertEquals(Optional.of("Respond within approximately 24 seconds"),
                TransferPresentation.countdownLabel(offered, after6s));

        long after29s = offerStart + TimeUnit.SECONDS.toNanos(29);
        assertEquals(Optional.of(1L), TransferPresentation.estimatedRemainingOfferSeconds(offered, after29s));
        assertEquals(Optional.of("Respond within approximately 1 second"),
                TransferPresentation.countdownLabel(offered, after29s));

        long after35s = offerStart + TimeUnit.SECONDS.toNanos(35);
        assertEquals(Optional.of(0L), TransferPresentation.estimatedRemainingOfferSeconds(offered, after35s));
        assertEquals(Optional.of("Offer expiring shortly — awaiting server timeout"),
                TransferPresentation.countdownLabel(offered, after35s));

        // Outgoing or non-OFFERED transfers must never display an incoming countdown.
        ClientModels.TransferItem outgoing = new ClientModels.TransferItem(
                tid, tid, ClientModels.TransferDirection.OUTGOING, peer, "bob", "file.png",
                null, 0, 2048, ClientModels.TransferStatus.OFFERING, "Offering", null, false, offerStart);
        assertTrue(TransferPresentation.countdownLabel(outgoing, after6s).isEmpty());

        ClientModels.TransferItem transferring = new ClientModels.TransferItem(
                tid, tid, ClientModels.TransferDirection.INCOMING, peer, "eve", "file.png",
                null, 512, 2048, ClientModels.TransferStatus.TRANSFERRING, "Transferring", null, false, 0L);
        assertTrue(TransferPresentation.countdownLabel(transferring, after6s).isEmpty());
    }
}
