import type {
  StoredResponseRecord,
  StoredResponseStatus,
  ResponseReviewCheckpoint,
  ResponseResumeIntent,
  ResponseTerminalReceipt,
} from '~/models/response';
import {
  responseScopeKeys,
  isResponseScopeClosed,
  initializeResponseScopes,
} from './responseScope';
import { getTenantId, SYSTEM_TENANT_ID } from '~/config/tenantContext';
import { createResponseScopeModel } from '~/models/responseScope';
import { createStoredResponseModel } from '~/models/response';
import { createIndexesWithRetry } from '~/utils/retry';

export type {
  StoredResponseRecord,
  StoredResponseStatus,
  ResponseReviewCheckpoint,
  ResponseResumeIntent,
  ResponseTerminalReceipt,
} from '~/models/response';

export interface ResponseOwner {
  userId: string;
  tenantId?: string;
}

export interface ResponseReservation extends ResponseOwner {
  responseId: string;
  requestKey: string;
  fingerprint: string;
  agentId: string;
  sourceKeyId?: string;
  requestJSON: string;
  conversationId: string;
  parentMessageId: string;
}

export type ResponseReservationResult =
  | { ok: true; record: StoredResponseRecord; replayed: boolean }
  | { ok: false; code: 'idempotency_conflict' | 'response_unavailable' };

export type ResponseMutationResult =
  | { ok: true; record: StoredResponseRecord }
  | { ok: false; code: 'response_conflict' };

export interface ResponseResumeReservation
  extends Omit<ResponseResumeIntent, 'state' | 'reservedAt' | 'admittedAt'> {
  generationCreatedAt: number;
}

/** Replays are receipts, not permission to enqueue or execute terminal/cancelled runs. */
export type ResponseResumeResult =
  | { ok: true; record: StoredResponseRecord; replayed: boolean }
  | { ok: false; code: 'resume_conflict' };

export interface ResponseStorage {
  stageTerminal(
    owner: ResponseOwner,
    responseId: string,
    revision: number,
    receipt: ResponseTerminalReceipt,
  ): Promise<ResponseMutationResult>;
  finishTerminal(
    owner: ResponseOwner,
    responseId: string,
    revision: number,
    receipt: ResponseTerminalReceipt,
  ): Promise<ResponseMutationResult>;
  publishReview(
    owner: ResponseOwner,
    responseId: string,
    revision: number,
    generationCreatedAt: number,
    review: ResponseReviewCheckpoint,
    responseJSON: string,
  ): Promise<ResponseMutationResult>;
  reserveResume(
    owner: ResponseOwner,
    responseId: string,
    input: ResponseResumeReservation,
    now: number,
  ): Promise<ResponseResumeResult>;
  admitResume(
    owner: ResponseOwner,
    responseId: string,
    intentId: string,
    generationCreatedAt: number,
    providerExecutionId: string,
    responseJSON: string,
    now: number,
  ): Promise<ResponseMutationResult>;
  reserve(input: ResponseReservation): Promise<ResponseReservationResult>;
  read(owner: ResponseOwner, responseId: string): Promise<StoredResponseRecord | null>;
  attachDelivery(
    owner: ResponseOwner,
    responseId: string,
    deliveryKey: string,
  ): Promise<ResponseMutationResult>;
  publish(
    owner: ResponseOwner,
    responseId: string,
    revision: number,
    update: { status: StoredResponseStatus; responseJSON: string; generationCreatedAt?: number },
  ): Promise<ResponseMutationResult>;
  requestCancellation(
    owner: ResponseOwner,
    responseId: string,
    now: number,
  ): Promise<StoredResponseRecord | null>;
}

const active = ['queued', 'in_progress', 'requires_action'];

function sameTerminal(a: ResponseTerminalReceipt, b: ResponseTerminalReceipt): boolean {
  return (
    a.generationCreatedAt === b.generationCreatedAt &&
    a.providerExecutionId === b.providerExecutionId &&
    a.ownerId === b.ownerId &&
    a.status === b.status &&
    a.responseJSON === b.responseJSON &&
    a.checkpointNamespace === b.checkpointNamespace &&
    a.checkpointConfigJSON === b.checkpointConfigJSON
  );
}

/** Authorization precedes this storage boundary; every read/write remains owner- and tenant-scoped. */
export function createResponseStorage(mongoose: typeof import('mongoose')): ResponseStorage {
  const model = createStoredResponseModel(mongoose);
  let indexPromise: Promise<void> | undefined;
  function ensureIndexes() {
    indexPromise ??= createIndexesWithRetry(model)
      .then(() => undefined)
      .catch((error: unknown) => {
        indexPromise = undefined;
        throw error;
      });
    return indexPromise;
  }
  function scope(owner: ResponseOwner) {
    if (!owner.userId || owner.tenantId === '__SYSTEM__')
      throw new TypeError('Invalid response owner');
    const tenantId = getTenantId();
    if (tenantId != null && tenantId !== SYSTEM_TENANT_ID && tenantId !== owner.tenantId)
      throw new TypeError('Response tenant does not match authenticated scope');
    // Explicit null excludes tenant-bearing records in single-tenant mode too.
    return { userId: owner.userId, tenantId: owner.tenantId ?? null };
  }
  return {
    async stageTerminal(owner, responseId, revision, receipt) {
      const pendingTerminal: ResponseTerminalReceipt = {
        generationCreatedAt: receipt.generationCreatedAt,
        providerExecutionId: receipt.providerExecutionId,
        ...(receipt.ownerId == null ? {} : { ownerId: receipt.ownerId }),
        status: receipt.status,
        responseJSON: receipt.responseJSON,
        ...(receipt.checkpointNamespace == null
          ? {}
          : { checkpointNamespace: receipt.checkpointNamespace }),
        ...(receipt.checkpointConfigJSON == null
          ? {}
          : { checkpointConfigJSON: receipt.checkpointConfigJSON }),
      };
      const record = await model
        .findOneAndUpdate(
          {
            ...scope(owner),
            responseId,
            revision,
            generationCreatedAt: receipt.generationCreatedAt,
            status: { $in: active },
            pendingTerminal: { $exists: false },
            $or: [
              {
                'resume.state': 'admitted',
                'resume.providerExecutionId': receipt.providerExecutionId,
              },
              {
                'resume.state': { $ne: 'admitted' },
                'review.providerExecutionId': receipt.providerExecutionId,
              },
              { review: { $exists: false }, resume: { $exists: false } },
            ],
          },
          { $set: { pendingTerminal }, $inc: { revision: 1 } },
          { new: true, runValidators: true },
        )
        .lean();
      if (record) return { ok: true, record };
      const existing = await model.findOne({ ...scope(owner), responseId }).lean();
      if (
        existing?.generationCreatedAt === receipt.generationCreatedAt &&
        existing.pendingTerminal &&
        sameTerminal(existing.pendingTerminal, pendingTerminal)
      )
        return { ok: true, record: existing };
      return { ok: false, code: 'response_conflict' };
    },
    async finishTerminal(owner, responseId, revision, receipt) {
      const record = await model
        .findOneAndUpdate(
          {
            ...scope(owner),
            responseId,
            revision,
            generationCreatedAt: receipt.generationCreatedAt,
            status: { $in: active },
            'pendingTerminal.generationCreatedAt': receipt.generationCreatedAt,
            'pendingTerminal.providerExecutionId': receipt.providerExecutionId,
            'pendingTerminal.ownerId': receipt.ownerId ?? { $exists: false },
            'pendingTerminal.status': receipt.status,
            'pendingTerminal.responseJSON': receipt.responseJSON,
            'pendingTerminal.checkpointNamespace': receipt.checkpointNamespace ?? {
              $exists: false,
            },
            'pendingTerminal.checkpointConfigJSON': receipt.checkpointConfigJSON ?? {
              $exists: false,
            },
          },
          {
            $set: { status: receipt.status, responseJSON: receipt.responseJSON },
            $unset: { pendingTerminal: 1 },
            $inc: { revision: 1 },
          },
          { new: true },
        )
        .lean();
      return record ? { ok: true, record } : { ok: false, code: 'response_conflict' };
    },
    async publishReview(owner, responseId, revision, generationCreatedAt, review, responseJSON) {
      const record = await model
        .findOneAndUpdate(
          {
            ...scope(owner),
            responseId,
            revision,
            generationCreatedAt,
            status: { $in: ['in_progress', 'requires_action'] },
            pendingTerminal: { $exists: false },
            cancellationRequestedAt: { $exists: false },
            $or: [
              { review: { $exists: false }, resume: { $exists: false } },
              {
                'review.actionId': review.actionId,
                'review.providerExecutionId': review.providerExecutionId,
                resume: { $exists: false },
              },
              {
                'resume.state': 'admitted',
                'resume.providerExecutionId': review.providerExecutionId,
              },
            ],
          },
          {
            $set: {
              status: 'requires_action',
              review: {
                actionId: review.actionId,
                providerExecutionId: review.providerExecutionId,
                expiresAt: review.expiresAt,
                partialResponseJSON: review.partialResponseJSON,
                ...(review.visibleOutputTokens == null
                  ? {}
                  : { visibleOutputTokens: review.visibleOutputTokens }),
                ...(review.pendingActionJSON == null
                  ? {}
                  : { pendingActionJSON: review.pendingActionJSON }),
                ...(review.checkpointConfigJSON == null
                  ? {}
                  : { checkpointConfigJSON: review.checkpointConfigJSON }),
                ...(review.checkpointId == null ? {} : { checkpointId: review.checkpointId }),
                ...(review.checkpointNs == null ? {} : { checkpointNs: review.checkpointNs }),
              },
              responseJSON,
            },
            $unset: { resume: 1 },
            $inc: { revision: 1 },
          },
          { new: true, runValidators: true },
        )
        .lean();
      return record ? { ok: true, record } : { ok: false, code: 'response_conflict' };
    },
    async reserveResume(owner, responseId, input, now) {
      if (!Number.isFinite(now) || now <= 0) throw new TypeError('Invalid resume reservation time');
      if (input.providerExecutionId === input.previousProviderExecutionId)
        throw new TypeError('Resume requires a new provider execution identity');
      const record = await model
        .findOneAndUpdate(
          {
            ...scope(owner),
            responseId,
            generationCreatedAt: input.generationCreatedAt,
            status: 'requires_action',
            pendingTerminal: { $exists: false },
            cancellationRequestedAt: { $exists: false },
            resume: { $exists: false },
            'review.actionId': input.actionId,
            'review.providerExecutionId': input.previousProviderExecutionId,
            'review.expiresAt': { $gt: now },
          },
          {
            $set: {
              resume: {
                intentId: input.intentId,
                actionId: input.actionId,
                previousProviderExecutionId: input.previousProviderExecutionId,
                providerExecutionId: input.providerExecutionId,
                fingerprint: input.fingerprint,
                resolutionJSON: input.resolutionJSON,
                ...(input.decisionJSON == null ? {} : { decisionJSON: input.decisionJSON }),
                state: 'reserved',
                reservedAt: now,
              },
            },
            $inc: { revision: 1 },
          },
          { new: true, runValidators: true },
        )
        .lean();
      if (record) return { ok: true, record, replayed: false };
      const existing = await model.findOne({ ...scope(owner), responseId }).lean();
      const intent = existing?.resume;
      if (
        existing?.generationCreatedAt === input.generationCreatedAt &&
        intent &&
        intent.actionId === input.actionId &&
        intent.previousProviderExecutionId === input.previousProviderExecutionId &&
        intent.fingerprint === input.fingerprint &&
        intent.resolutionJSON === input.resolutionJSON &&
        intent.decisionJSON === input.decisionJSON
      )
        return { ok: true, record: existing, replayed: true };
      return { ok: false, code: 'resume_conflict' };
    },
    async admitResume(
      owner,
      responseId,
      intentId,
      generationCreatedAt,
      providerExecutionId,
      responseJSON,
      now,
    ) {
      if (!Number.isFinite(now) || now <= 0) throw new TypeError('Invalid resume admission time');
      const record = await model
        .findOneAndUpdate(
          {
            ...scope(owner),
            responseId,
            generationCreatedAt,
            status: 'requires_action',
            pendingTerminal: { $exists: false },
            cancellationRequestedAt: { $exists: false },
            'resume.intentId': intentId,
            'resume.providerExecutionId': providerExecutionId,
            'resume.state': 'reserved',
          },
          {
            $set: {
              status: 'in_progress',
              responseJSON,
              'resume.state': 'admitted',
              'resume.admittedAt': now,
            },
            $inc: { revision: 1 },
          },
          { new: true },
        )
        .lean();
      if (record) return { ok: true, record };
      const existing = await model.findOne({ ...scope(owner), responseId }).lean();
      if (
        existing?.generationCreatedAt === generationCreatedAt &&
        existing.resume?.intentId === intentId &&
        existing.resume.providerExecutionId === providerExecutionId &&
        existing.resume.state === 'admitted'
      )
        return { ok: true, record: existing };
      return { ok: false, code: 'response_conflict' };
    },
    async reserve(input) {
      await ensureIndexes();
      const filter = { ...scope(input), requestKey: input.requestKey };
      const prior = await model.findOne(filter).select('conversationId').lean();
      const keys = responseScopeKeys(input, prior?.conversationId ?? input.conversationId);
      await initializeResponseScopes(mongoose, keys);
      const session = await mongoose.startSession();
      try {
        let result: ResponseReservationResult | undefined;
        await session.withTransaction(async () => {
          // A write, not a snapshot read: erasure waits for admitted insertions to commit.
          const scopes = createResponseScopeModel(mongoose);
          const touched = await scopes.updateOne(
            { _id: keys[0], closed: false },
            { $inc: { revision: 1 } },
            { session },
          );
          if (touched.matchedCount !== 1) {
            result = { ok: false, code: 'response_unavailable' };
            return;
          }
          const existing = await model.findOne(filter).session(session).lean();
          const conversationKey = responseScopeKeys(
            input,
            existing?.conversationId ?? input.conversationId,
          )[1];
          const conversation = await scopes.updateOne(
            { _id: conversationKey, closed: false },
            { $inc: { revision: 1 } },
            { session },
          );
          if (conversation.matchedCount !== 1) {
            result = { ok: false, code: 'response_unavailable' };
            return;
          }
          if (existing) {
            result =
              existing.fingerprint === input.fingerprint
                ? { ok: true, record: existing, replayed: true }
                : { ok: false, code: 'idempotency_conflict' };
            return;
          }
          const [created] = await model.create([{ ...input, status: 'queued', revision: 0 }], {
            session,
          });
          result = { ok: true, record: created.toObject(), replayed: false };
        });
        if (!result) throw new Error('Response reservation did not commit');
        return result;
      } catch (error) {
        if (!(error instanceof mongoose.mongo.MongoServerError) || error.code !== 11000)
          throw error;
        const existing = await model.findOne(filter).lean();
        if (!existing) throw error;
        if (await isResponseScopeClosed(mongoose, input, existing.conversationId))
          return { ok: false, code: 'response_unavailable' };
        return existing.fingerprint === input.fingerprint
          ? { ok: true, record: existing, replayed: true }
          : { ok: false, code: 'idempotency_conflict' };
      } finally {
        await session.endSession();
      }
    },
    async read(owner, responseId) {
      return model.findOne({ ...scope(owner), responseId }).lean();
    },
    async attachDelivery(owner, responseId, deliveryKey) {
      const record = await model
        .findOneAndUpdate(
          {
            ...scope(owner),
            responseId,
            status: { $in: active },
            $or: [{ deliveryKey: { $exists: false } }, { deliveryKey }],
          },
          { $set: { deliveryKey } },
          { new: true },
        )
        .lean();
      return record ? { ok: true, record } : { ok: false, code: 'response_conflict' };
    },
    async publish(owner, responseId, revision, update) {
      if (
        update.generationCreatedAt != null &&
        (!Number.isSafeInteger(update.generationCreatedAt) || update.generationCreatedAt <= 0)
      )
        throw new TypeError('Invalid response generation identity');
      let predecessors = active;
      if (update.status === 'queued') predecessors = ['queued'];
      if (update.status === 'in_progress') predecessors = ['queued', 'in_progress'];
      const record = await model
        .findOneAndUpdate(
          {
            ...scope(owner),
            responseId,
            revision,
            status: { $in: predecessors },
            pendingTerminal: { $exists: false },
            ...(update.status === 'in_progress'
              ? {
                  $and: [
                    {
                      $or: [
                        { status: 'in_progress' },
                        { cancellationRequestedAt: { $exists: false } },
                      ],
                    },
                    ...(update.generationCreatedAt == null
                      ? [{ generationCreatedAt: { $gt: 0 } }]
                      : []),
                  ],
                }
              : {}),
            ...(update.generationCreatedAt == null
              ? {}
              : {
                  $or: [
                    { generationCreatedAt: { $exists: false } },
                    { generationCreatedAt: update.generationCreatedAt },
                  ],
                }),
          },
          { $set: update, $inc: { revision: 1 } },
          { new: true },
        )
        .lean();
      return record ? { ok: true, record } : { ok: false, code: 'response_conflict' };
    },
    async requestCancellation(owner, responseId, now) {
      if (!Number.isFinite(now) || now <= 0) throw new TypeError('Invalid cancellation time');
      await model.updateOne(
        {
          ...scope(owner),
          responseId,
          status: { $in: active },
          cancellationRequestedAt: { $exists: false },
          pendingTerminal: { $exists: false },
        },
        { $set: { cancellationRequestedAt: now } },
      );
      return model.findOne({ ...scope(owner), responseId }).lean();
    },
  };
}
