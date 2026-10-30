---- MODULE ik ----
(***************************************************************************)
(* Clean-room TLA+ model of the Noise IK handshake in internal/noise/ik.go.*)
(*                                                                         *)
(* Handshake outline (spec pattern -> e, es, s, ss / <- e, ee, se):        *)
(*   IWrite  models WriteRequest  (runs once; guards wrote/failed/done).   *)
(*   RRead   models ReadRequest   (runs once; guards read/failed/done).    *)
(*   RWrite* models WriteResponse (success mirrors Split keys: send=k2,    *)
(*             recv=k1; failure poisons the responder).                    *)
(*   IRead*  models ReadResponse  (success gives send=k1, recv=k2; failure *)
(*             poisons the initiator).                                     *)
(*                                                                         *)
(* Abstractions that keep the state space finite for TLC:                  *)
(*   - Handshake payloads range over {0, 1}.                               *)
(*   - Transport keys are small ints: 0 = none yet, K1 = 1, K2 = 2.        *)
(*     No action forges keys; only Split assigns K1/K2, mirrored.          *)
(*   - Transport nonces range over 0..MaxNonce with MaxNonce = 3 standing  *)
(*     in for 2^64-1. The step past the cap moves to terminal err.         *)
(*   - Rekey bumps an epoch counter rk and leaves keys and nonces alone,   *)
(*     which records the ik.go property that rekey never resets the nonce. *)
(*   - The wire retains messages (haveReq/haveResp/haveIR/haveRI), so      *)
(*     Drop is a no-state-change step with redelivery still possible, and  *)
(*     Duplicate replay of a consumed handshake slot is rejected by the    *)
(*     state guards (~read / ~done). Transport replay uses a stale nonce,  *)
(*     which RDecFail/IDecFail reject without incrementing the nonce.      *)
(***************************************************************************)
EXTENDS Integers

VARIABLES
    iWrote, iFailed, iDone,
    rRead, rFailed, rDone,
    iSend, iRecv, rSend, rRecv,
    iSendN, iRecvN, rSendN, rRecvN,
    haveReq, reqPl, haveResp, respPl,
    haveIR, nonceIR, plIR,
    haveRI, nonceRI, plRI,
    rk, err

vars ==
    <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
      iSend, iRecv, rSend, rRecv,
      iSendN, iRecvN, rSendN, rRecvN,
      haveReq, reqPl, haveResp, respPl,
      haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
      rk, err>>

MaxNonce == 3
NoKey == 0
K1 == 1
K2 == 2

TypeOK ==
    /\ iWrote \in BOOLEAN
    /\ iFailed \in BOOLEAN
    /\ iDone \in BOOLEAN
    /\ rRead \in BOOLEAN
    /\ rFailed \in BOOLEAN
    /\ rDone \in BOOLEAN
    /\ iSend \in {NoKey, K1, K2}
    /\ iRecv \in {NoKey, K1, K2}
    /\ rSend \in {NoKey, K1, K2}
    /\ rRecv \in {NoKey, K1, K2}
    /\ iSendN \in 0 .. MaxNonce
    /\ iRecvN \in 0 .. MaxNonce
    /\ rSendN \in 0 .. MaxNonce
    /\ rRecvN \in 0 .. MaxNonce
    /\ haveReq \in BOOLEAN
    /\ reqPl \in {0, 1}
    /\ haveResp \in BOOLEAN
    /\ respPl \in {0, 1}
    /\ haveIR \in BOOLEAN
    /\ nonceIR \in 0 .. MaxNonce
    /\ plIR \in {0, 1}
    /\ haveRI \in BOOLEAN
    /\ nonceRI \in 0 .. MaxNonce
    /\ plRI \in {0, 1}
    /\ rk \in 0 .. 2
    /\ err \in BOOLEAN

Init ==
    /\ iWrote = FALSE
    /\ iFailed = FALSE
    /\ iDone = FALSE
    /\ rRead = FALSE
    /\ rFailed = FALSE
    /\ rDone = FALSE
    /\ iSend = NoKey
    /\ iRecv = NoKey
    /\ rSend = NoKey
    /\ rRecv = NoKey
    /\ iSendN = 0
    /\ iRecvN = 0
    /\ rSendN = 0
    /\ rRecvN = 0
    /\ haveReq = FALSE
    /\ reqPl = 0
    /\ haveResp = FALSE
    /\ respPl = 0
    /\ haveIR = FALSE
    /\ nonceIR = 0
    /\ plIR = 0
    /\ haveRI = FALSE
    /\ nonceRI = 0
    /\ plRI = 0
    /\ rk = 0
    /\ err = FALSE

(* Initiator writes -> e, es, s, ss exactly once. *)
IWrite ==
    /\ ~iFailed /\ ~iDone /\ ~iWrote /\ ~err
    /\ iWrote' = TRUE
    /\ haveReq' = TRUE
    /\ (\E p \in {0, 1} : reqPl' = p)
    /\ UNCHANGED <<iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

(* Responder consumes the request exactly once. *)
RRead ==
    /\ ~rFailed /\ ~rDone /\ ~rRead /\ ~err
    /\ haveReq
    /\ rRead' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

(* Responder emits <- e, ee, se; Split order mirrored: send=k2, recv=k1. *)
RWriteOk ==
    /\ ~rFailed /\ ~rDone /\ rRead /\ ~err
    /\ rDone' = TRUE
    /\ rSend' = K2
    /\ rRecv' = K1
    /\ haveResp' = TRUE
    /\ (\E p \in {0, 1} : respPl' = p)
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed,
                   iSend, iRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

RWriteFail ==
    /\ ~rFailed /\ ~rDone /\ rRead /\ ~err
    /\ rFailed' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

(* Initiator consumes the response; success splits send=k1, recv=k2. *)
IReadOk ==
    /\ ~iFailed /\ ~iDone /\ iWrote /\ ~err
    /\ haveResp
    /\ iDone' = TRUE
    /\ iSend' = K1
    /\ iRecv' = K2
    /\ UNCHANGED <<iWrote, iFailed, rRead, rFailed, rDone,
                   rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

IReadFail ==
    /\ ~iFailed /\ ~iDone /\ iWrote /\ ~err
    /\ haveResp
    /\ iFailed' = TRUE
    /\ UNCHANGED <<iWrote, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

(* Transport I->R: ciphertext carries the current send nonce, then +1. *)
IEncOK ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ ~haveIR
    /\ iSendN < MaxNonce
    /\ iSendN' = iSendN + 1
    /\ nonceIR' = iSendN
    /\ (\E p \in {0, 1} : plIR' = p)
    /\ haveIR' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveRI, nonceRI, plRI,
                   rk, err>>

(* Nonce past the cap fails closed into terminal err, never encrypts. *)
IEncOverflow ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ ~haveIR
    /\ iSendN = MaxNonce
    /\ err' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk>>

(* Decrypt succeeds only when the ciphertext nonce equals the recv nonce. *)
RDecOK ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ haveIR
    /\ nonceIR = rRecvN
    /\ rRecvN < MaxNonce
    /\ rRecvN' = rRecvN + 1
    /\ haveIR' = FALSE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN,
                   haveReq, reqPl, haveResp, respPl,
                   nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

RDecOverflow ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ haveIR
    /\ nonceIR = rRecvN
    /\ rRecvN = MaxNonce
    /\ err' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk>>

(* Forged or replayed ciphertext fails closed: consumed, nonce unchanged. *)
RDecFail ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ haveIR
    /\ nonceIR # rRecvN
    /\ haveIR' = FALSE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

(* Transport R->I, mirror image using rSendN and iRecvN. *)
REncOK ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ ~haveRI
    /\ rSendN < MaxNonce
    /\ rSendN' = rSendN + 1
    /\ nonceRI' = rSendN
    /\ (\E p \in {0, 1} : plRI' = p)
    /\ haveRI' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR,
                   rk, err>>

REncOverflow ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ ~haveRI
    /\ rSendN = MaxNonce
    /\ err' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk>>

IDecOK ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ haveRI
    /\ nonceRI = iRecvN
    /\ iRecvN < MaxNonce
    /\ iRecvN' = iRecvN + 1
    /\ haveRI' = FALSE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, nonceRI, plRI,
                   rk, err>>

IDecOverflow ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ haveRI
    /\ nonceRI = iRecvN
    /\ iRecvN = MaxNonce
    /\ err' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk>>

IDecFail ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ haveRI
    /\ nonceRI # iRecvN
    /\ haveRI' = FALSE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, nonceRI, plRI,
                   rk, err>>

(* Rekey bumps the epoch only; nonces and keys are preserved in sync. *)
Rekey ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ rk < 2
    /\ rk' = rk + 1
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   err>>

(* Adversary: drop leaves all state unchanged; redelivery stays possible. *)
AdvDrop == UNCHANGED vars

(* Adversary: replay of the request after it was read; RRead guard rejects. *)
AdvDupReq ==
    /\ haveReq /\ rRead
    /\ UNCHANGED vars

(* Adversary: replay of the response after completion; IRead guard rejects. *)
AdvDupResp ==
    /\ haveResp /\ iDone
    /\ UNCHANGED vars

(* Adversary: replay a stale I->R ciphertext; RDecFail must reject it. *)
AdvReplayIR ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ ~haveIR /\ rRecvN > 0
    /\ (\E n \in 0 .. (rRecvN - 1) : nonceIR' = n)
    /\ (\E p \in {0, 1} : plIR' = p)
    /\ haveIR' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveRI, nonceRI, plRI,
                   rk, err>>

(* Adversary: replay a stale R->I ciphertext; IDecFail must reject it. *)
AdvReplayRI ==
    /\ iDone /\ rDone /\ ~iFailed /\ ~rFailed /\ ~err
    /\ ~haveRI /\ iRecvN > 0
    /\ (\E n \in 0 .. (iRecvN - 1) : nonceRI' = n)
    /\ (\E p \in {0, 1} : plRI' = p)
    /\ haveRI' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR,
                   rk, err>>

(* Adversary: tamper forces the targeted party into failed. *)
AdvTamperI ==
    /\ ~iFailed /\ ~iDone /\ ~err
    /\ iFailed' = TRUE
    /\ UNCHANGED <<iWrote, iDone, rRead, rFailed, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

AdvTamperR ==
    /\ ~rFailed /\ ~rDone /\ ~err
    /\ rFailed' = TRUE
    /\ UNCHANGED <<iWrote, iFailed, iDone, rRead, rDone,
                   iSend, iRecv, rSend, rRecv,
                   iSendN, iRecvN, rSendN, rRecvN,
                   haveReq, reqPl, haveResp, respPl,
                   haveIR, nonceIR, plIR, haveRI, nonceRI, plRI,
                   rk, err>>

(* Terminal stutter: failed, error, and completed states have a successor. *)
Done == UNCHANGED vars

Next ==
    \/ IWrite
    \/ RRead
    \/ RWriteOk
    \/ RWriteFail
    \/ IReadOk
    \/ IReadFail
    \/ IEncOK
    \/ IEncOverflow
    \/ RDecOK
    \/ RDecOverflow
    \/ RDecFail
    \/ REncOK
    \/ REncOverflow
    \/ IDecOK
    \/ IDecOverflow
    \/ IDecFail
    \/ Rekey
    \/ AdvDupReq
    \/ AdvDupResp
    \/ AdvReplayIR
    \/ AdvReplayRI
    \/ AdvDrop
    \/ AdvTamperI
    \/ AdvTamperR
    \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(* Split keys are mirrored: initiator send matches responder recv. *)
KeyMirror == (iDone /\ rDone) => (iSend = rRecv /\ iRecv = rSend)

(* Failed and completed are mutually exclusive per party. *)
TerminalFailed == (iFailed => ~iDone) /\ (rFailed => ~rDone)

(* Completion implies the single allowed write/read already happened. *)
NoDoubleWrite == (iDone => iWrote) /\ (rDone => rRead)

(* Nonces stay inside the finite cap, including on the error path. *)
NonceMonotonic ==
    /\ iSendN \in 0 .. MaxNonce
    /\ iRecvN \in 0 .. MaxNonce
    /\ rSendN \in 0 .. MaxNonce
    /\ rRecvN \in 0 .. MaxNonce

NoFail == ~iFailed /\ ~rFailed /\ ~err
BothDone == iDone /\ rDone

(* A fair schedule that never fails and never errors completes. *)
Liveness == ([] NoFail) => (<> BothDone)
====
