import type { ResponseOwner, ResponseStorage, StoredResponseRecord } from '@librechat/data-schemas';
import type { GenerationJobManagerClass } from '~/stream/GenerationJobManager';
import { buildPendingBackgroundResponse } from './background';
import { createResponseCompletion } from './completion';

export interface ResponseRetirementDependencies {
  storage: ResponseStorage;
  jobs: Pick<GenerationJobManagerClass, 'getJob' | 'abortJob'>;
  /** Required before a durably paused response may publish cancellation. Must be idempotent. */
  cleanupCheckpoint?: (record: StoredResponseRecord, checkpointNamespace: string) => Promise<void>;
  recoverTerminal?: (record: StoredResponseRecord) => Promise<StoredResponseRecord>;
  now?: () => number;
}

const terminal = (record: StoredResponseRecord) =>
  ['completed', 'failed', 'incomplete', 'cancelled'].includes(record.status);

/** Internal cleanup authority: may stop one owned response, never execute or disclose its output. */
export function createResponseRetirement(
  deps: ResponseRetirementDependencies,
): (owner: ResponseOwner, responseId: string) => Promise<'stopped' | 'pending'> {
  const recover =
    deps.recoverTerminal ??
    (deps.cleanupCheckpoint
      ? createResponseCompletion({
          storage: deps.storage,
          cleanupCheckpoint: deps.cleanupCheckpoint,
        }).finish
      : undefined);
  return async (owner, responseId) => {
    let record = await deps.storage.requestCancellation(
      owner,
      responseId,
      (deps.now ?? Date.now)(),
    );
    if (!record) throw new Error('Response retirement identity unavailable');
    if (terminal(record)) {
      await recover?.(record);
      return 'stopped';
    }
    if (record.pendingTerminal) {
      if (!recover) return 'pending';
      return terminal(await recover(record)) ? 'stopped' : 'pending';
    }
    if (record.generationCreatedAt == null) {
      const response = {
        ...buildPendingBackgroundResponse(record),
        status: 'cancelled' as const,
        completed_at: Math.floor((deps.now ?? Date.now)() / 1000),
      };
      const cancelled = await deps.storage.publish(owner, responseId, record.revision, {
        status: 'cancelled',
        responseJSON: JSON.stringify(response),
      });
      // This CAS beats enrollment publication, which must succeed before provider startup.
      if (cancelled.ok) return 'stopped';
      record = await deps.storage.read(owner, responseId);
      if (!record) throw new Error('Response disappeared during retirement');
      if (terminal(record)) return 'stopped';
    }
    const job = await deps.jobs.getJob(responseId);
    if (!job) return 'pending';
    if (
      record.generationCreatedAt == null ||
      job.createdAt !== record.generationCreatedAt ||
      job.metadata.userId !== record.userId ||
      job.metadata.tenantId !== record.tenantId ||
      job.metadata.agent_id !== record.agentId ||
      job.metadata.endpoint !== 'responses' ||
      job.metadata.conversationId !== record.conversationId ||
      job.metadata.responseMessageId !== record.responseId
    )
      throw new Error('Response retirement generation changed');
    const epoch = record.generationCreatedAt;
    const agentId = record.agentId;
    const conversationId = record.conversationId;
    const providerExecutionId = job.metadata.providerExecutionId;
    const providerExecutionOwnerId = job.metadata.providerExecutionOwnerId;
    async function stageCancellation(): Promise<void> {
      const stopped = await deps.jobs.getJob(responseId);
      const proof = stopped?.metadata;
      if (
        !recover ||
        stopped?.status !== 'aborted' ||
        !proof ||
        proof.providerDrained !== true ||
        stopped.createdAt !== epoch ||
        proof.userId !== owner.userId ||
        proof.tenantId !== owner.tenantId ||
        proof.agent_id !== agentId ||
        proof.endpoint !== 'responses' ||
        proof.responseMessageId !== responseId ||
        proof.conversationId !== conversationId ||
        proof.providerExecutionId !== providerExecutionId ||
        proof.providerExecutionOwnerId !== providerExecutionOwnerId ||
        !proof.providerExecutionId
      )
        return;
      const current = await deps.storage.read(owner, responseId);
      if (!current || current.generationCreatedAt !== epoch)
        throw new Error('Response retirement journal changed');
      if (terminal(current) || current.pendingTerminal) return;
      if (current.cancellationRequestedAt == null)
        throw new Error('Response cancellation intent is unavailable');
      if (current.review && (!proof.checkpointNamespace || !deps.cleanupCheckpoint)) return;
      const response = {
        ...buildPendingBackgroundResponse(current),
        model: current.agentId,
        status: 'cancelled' as const,
        completed_at: Math.floor((deps.now ?? Date.now)() / 1000),
      };
      const receipt = {
        generationCreatedAt: epoch,
        providerExecutionId: proof.providerExecutionId,
        ...(proof.providerExecutionOwnerId == null
          ? {}
          : { ownerId: proof.providerExecutionOwnerId }),
        status: 'cancelled' as const,
        responseJSON: JSON.stringify(response),
        ...(current.review
          ? {
              checkpointNamespace: proof.checkpointNamespace,
              ...(current.review.checkpointConfigJSON == null
                ? {}
                : { checkpointConfigJSON: current.review.checkpointConfigJSON }),
            }
          : {}),
      };
      try {
        const staged = await deps.storage.stageTerminal(
          owner,
          responseId,
          current.revision,
          receipt,
        );
        if (!staged.ok) throw new Error('Response retirement receipt conflicted');
      } catch (error) {
        const saved = await deps.storage.read(owner, responseId);
        if (
          !saved ||
          saved.generationCreatedAt !== epoch ||
          (!terminal(saved) &&
            (saved.pendingTerminal?.providerExecutionId !== receipt.providerExecutionId ||
              saved.pendingTerminal.responseJSON !== receipt.responseJSON))
        )
          throw error;
      }
    }
    await deps.jobs.abortJob(responseId, {
      expectedCreatedAt: record.generationCreatedAt,
      awaitProviderDrain: true,
      retainTerminalPersistence: true,
      beforePublish: stageCancellation,
    });
    // Runtime publication includes provider drain and trailing writes; a missing job alone is not proof.
    let settled = await deps.storage.read(owner, responseId);
    if (settled && terminal(settled)) {
      await recover?.(settled);
      return 'stopped';
    }
    if (settled?.pendingTerminal && recover)
      return terminal(await recover(settled)) ? 'stopped' : 'pending';
    // A repeated Stop can recover a failed runtime receipt only from the exact retained,
    // aborted and drained provider. Completed jobs and absent jobs are not cancellation proof.
    await stageCancellation();
    settled = await deps.storage.read(owner, responseId);
    if (settled?.pendingTerminal && recover) settled = await recover(settled);
    return settled && terminal(settled) ? 'stopped' : 'pending';
  };
}
