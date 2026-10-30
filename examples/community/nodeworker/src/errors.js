// Typed platform errors: a canonical Connect code plus a common.v1.ErrorInfo
// detail carrying the ErrorReason (docs/protocol.md, "Frontend mapping of
// platform errors" holds the reason -> code table).
import { Code, ConnectError } from "@connectrpc/connect";
import { ErrorInfoSchema, ErrorReason } from "../gen/edgerun/common/v1/common_pb.js";

const CODE_OF = {
  [ErrorReason.FUNCTION_NOT_FOUND]: Code.NotFound,
  [ErrorReason.ARTIFACT_NOT_FOUND]: Code.NotFound,
  [ErrorReason.NO_CAPACITY]: Code.ResourceExhausted,
  [ErrorReason.RESPONSE_TOO_LARGE]: Code.ResourceExhausted,
  [ErrorReason.DEADLINE_EXCEEDED]: Code.DeadlineExceeded,
  [ErrorReason.FUNCTION_TRAPPED]: Code.Aborted,
  [ErrorReason.ABI_MISMATCH]: Code.FailedPrecondition,
  [ErrorReason.REQUEST_TOO_LARGE]: Code.InvalidArgument,
  [ErrorReason.INTERNAL]: Code.Internal,
  [ErrorReason.EGRESS_DENIED]: Code.PermissionDenied,
  [ErrorReason.EGRESS_FAILED]: Code.Unavailable,
};

export { ErrorReason };

export function platformError(reason, message) {
  return new ConnectError(message, CODE_OF[reason] ?? Code.Internal, undefined, [
    { desc: ErrorInfoSchema, value: { reason, message } },
  ]);
}

/** A caller bug: invalid_argument with no reason. */
export function invalidArgument(message) {
  return new ConnectError(message, Code.InvalidArgument);
}

/** The reason carried by an error, or ERROR_REASON_UNSPECIFIED. */
export function reasonOf(err) {
  if (!(err instanceof ConnectError)) return ErrorReason.UNSPECIFIED;
  const [info] = err.findDetails(ErrorInfoSchema);
  return info ? info.reason : ErrorReason.UNSPECIFIED;
}
