//! Tonic service implementations.

#![allow(missing_docs)]

use ironledger_ledger::LedgerService;
use ironledger_reconciler::Reconciler;
use ironledger_storage_postgres::{PostgresBalanceViews, PostgresHistory};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::map::{
    admin_adjustment, command_result, complete_withdrawal, create_account, ledger_status,
    map_account, map_entry, parse_account_id, parse_asset, parse_uuid, record_deposit,
    reject_withdrawal, request_withdrawal, submit_journal, trade_settlement, trading_fee,
};
use crate::proto::{
    ledger_admin_service_server::LedgerAdminService,
    ledger_query_service_server::LedgerQueryService,
    ledger_write_service_server::LedgerWriteService, CreateAccountResponse, GetBalanceResponse,
    GetTransactionResponse, ListAccountEntriesResponse, RecordAdminAdjustmentResponse,
    RecordDepositResponse, RecordTradeSettlementResponse, RecordTradingFeeResponse,
    RejectWithdrawalResponse, RequestWithdrawalResponse, SubmitJournalEntryResponse,
    TriggerReconciliationResponse,
};
use crate::proto::{
    CompleteWithdrawalRequest, CompleteWithdrawalResponse, CreateAccountRequest, GetBalanceRequest,
    GetTransactionRequest, ListAccountEntriesRequest, RecordAdminAdjustmentRequest,
    RecordDepositRequest, RecordTradeSettlementRequest, RecordTradingFeeRequest,
    RejectWithdrawalRequest, RequestWithdrawalRequest, SubmitJournalEntryRequest,
    TriggerReconciliationRequest,
};

/// gRPC service state.
#[derive(Clone)]
pub struct GrpcServices {
    ledger: Arc<LedgerService>,
    reconciler: Arc<Reconciler<PostgresHistory, PostgresBalanceViews>>,
}

impl GrpcServices {
    /// Create shared gRPC handlers.
    pub fn new(
        ledger: Arc<LedgerService>,
        reconciler: Arc<Reconciler<PostgresHistory, PostgresBalanceViews>>,
    ) -> Self {
        Self { ledger, reconciler }
    }
}

#[tonic::async_trait]
impl LedgerWriteService for GrpcServices {
    async fn create_account(
        &self,
        request: Request<CreateAccountRequest>,
    ) -> Result<Response<CreateAccountResponse>, Status> {
        let command = create_account(request.into_inner())?;
        let outcome = self
            .ledger
            .create_account(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(CreateAccountResponse {
            account: Some(map_account(&outcome.value.account)),
            replayed: outcome.replayed,
        }))
    }

    async fn submit_journal_entry(
        &self,
        request: Request<SubmitJournalEntryRequest>,
    ) -> Result<Response<SubmitJournalEntryResponse>, Status> {
        let command = submit_journal(request.into_inner())?;
        let outcome = self
            .ledger
            .submit_journal_entry(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(SubmitJournalEntryResponse {
            result: Some(command_result(
                outcome.value.transaction_id,
                outcome.replayed,
                outcome.value.posted_at,
            )),
        }))
    }

    async fn record_deposit(
        &self,
        request: Request<RecordDepositRequest>,
    ) -> Result<Response<RecordDepositResponse>, Status> {
        let command = record_deposit(request.into_inner())?;
        let outcome = self
            .ledger
            .record_deposit(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(RecordDepositResponse {
            result: Some(command_result(
                outcome.value.transaction_id,
                outcome.replayed,
                outcome.value.posted_at,
            )),
        }))
    }

    async fn request_withdrawal(
        &self,
        request: Request<RequestWithdrawalRequest>,
    ) -> Result<Response<RequestWithdrawalResponse>, Status> {
        let command = request_withdrawal(request.into_inner())?;
        let outcome = self
            .ledger
            .request_withdrawal(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(RequestWithdrawalResponse {
            result: Some(command_result(
                outcome.value.transaction_id,
                outcome.replayed,
                outcome.value.posted_at,
            )),
        }))
    }

    async fn complete_withdrawal(
        &self,
        request: Request<CompleteWithdrawalRequest>,
    ) -> Result<Response<CompleteWithdrawalResponse>, Status> {
        let command = complete_withdrawal(request.into_inner())?;
        let outcome = self
            .ledger
            .complete_withdrawal(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(CompleteWithdrawalResponse {
            result: Some(command_result(
                outcome.value.transaction_id,
                outcome.replayed,
                outcome.value.posted_at,
            )),
        }))
    }

    async fn reject_withdrawal(
        &self,
        request: Request<RejectWithdrawalRequest>,
    ) -> Result<Response<RejectWithdrawalResponse>, Status> {
        let command = reject_withdrawal(request.into_inner())?;
        let outcome = self
            .ledger
            .reject_withdrawal(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(RejectWithdrawalResponse {
            result: Some(command_result(
                outcome.value.transaction_id,
                outcome.replayed,
                outcome.value.posted_at,
            )),
        }))
    }

    async fn record_trade_settlement(
        &self,
        request: Request<RecordTradeSettlementRequest>,
    ) -> Result<Response<RecordTradeSettlementResponse>, Status> {
        let command = trade_settlement(request.into_inner())?;
        let outcome = self
            .ledger
            .record_trade_settlement(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(RecordTradeSettlementResponse {
            result: Some(command_result(
                outcome.value.transaction_id,
                outcome.replayed,
                outcome.value.posted_at,
            )),
        }))
    }

    async fn record_trading_fee(
        &self,
        request: Request<RecordTradingFeeRequest>,
    ) -> Result<Response<RecordTradingFeeResponse>, Status> {
        let command = trading_fee(request.into_inner())?;
        let outcome = self
            .ledger
            .record_trading_fee(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(RecordTradingFeeResponse {
            result: Some(command_result(
                outcome.value.transaction_id,
                outcome.replayed,
                outcome.value.posted_at,
            )),
        }))
    }

    async fn record_admin_adjustment(
        &self,
        request: Request<RecordAdminAdjustmentRequest>,
    ) -> Result<Response<RecordAdminAdjustmentResponse>, Status> {
        let command = admin_adjustment(request.into_inner())?;
        let outcome = self
            .ledger
            .record_admin_adjustment(command)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(RecordAdminAdjustmentResponse {
            result: Some(command_result(
                outcome.value.transaction_id,
                outcome.replayed,
                outcome.value.posted_at,
            )),
        }))
    }
}

#[tonic::async_trait]
impl LedgerQueryService for GrpcServices {
    async fn get_transaction(
        &self,
        request: Request<GetTransactionRequest>,
    ) -> Result<Response<GetTransactionResponse>, Status> {
        let id = TransactionId::from_uuid(parse_uuid(
            &request.into_inner().transaction_id,
            "transaction_id",
        )?);
        let entry = self
            .ledger
            .get_transaction(id)
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(GetTransactionResponse {
            entry: Some(map_entry(&entry)),
        }))
    }

    async fn get_balance(
        &self,
        request: Request<GetBalanceRequest>,
    ) -> Result<Response<GetBalanceResponse>, Status> {
        let req = request.into_inner();
        let account_id = parse_account_id(&req.account_id, "account_id")?;
        let balances = if req.asset.is_empty() {
            self.ledger
                .get_balances(account_id, None)
                .await
                .map_err(ledger_status)?
        } else {
            let asset = parse_asset(&req.asset)?;
            self.ledger
                .get_balances(account_id, Some(&asset))
                .await
                .map_err(ledger_status)?
        };
        Ok(Response::new(GetBalanceResponse {
            balances: balances
                .into_iter()
                .map(|balance| crate::proto::Balance {
                    account_id: balance.account_id.to_string(),
                    asset: balance.asset.to_string(),
                    amount_atomic: balance.amount.raw().to_string(),
                    updated_at: balance.updated_at.to_rfc3339(),
                })
                .collect(),
        }))
    }

    async fn list_account_entries(
        &self,
        request: Request<ListAccountEntriesRequest>,
    ) -> Result<Response<ListAccountEntriesResponse>, Status> {
        let req = request.into_inner();
        let account_id = parse_account_id(&req.account_id, "account_id")?;
        let entries = self
            .ledger
            .list_account_entries(
                account_id,
                ironledger_ledger::Page::new(req.limit, req.offset),
            )
            .await
            .map_err(ledger_status)?;
        Ok(Response::new(ListAccountEntriesResponse {
            entries: entries.iter().map(map_entry).collect(),
        }))
    }
}

#[tonic::async_trait]
impl LedgerAdminService for GrpcServices {
    async fn trigger_reconciliation(
        &self,
        request: Request<TriggerReconciliationRequest>,
    ) -> Result<Response<TriggerReconciliationResponse>, Status> {
        let req = request.into_inner();
        let started = chrono::Utc::now();
        let report = self
            .reconciler
            .reconcile_authoritative()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let discrepancies = if req.summary_only {
            Vec::new()
        } else {
            report
                .discrepancies
                .iter()
                .map(|d| crate::proto::Discrepancy {
                    account_id: d.account_id.to_string(),
                    asset: d.asset.to_string(),
                    expected_atomic: d.expected.raw().to_string(),
                    actual_atomic: d.observed.raw().to_string(),
                    delta_atomic: (d.expected.raw() - d.observed.raw()).to_string(),
                    source: d.view.clone(),
                })
                .collect()
        };
        Ok(Response::new(TriggerReconciliationResponse {
            run_id: uuid::Uuid::new_v4().to_string(),
            checked_balances: report.postings_applied,
            balanced: report.is_clean(),
            discrepancies,
            started_at: started.to_rfc3339(),
            finished_at: report.finished_at.to_rfc3339(),
        }))
    }
}

use ironledger_domain::TransactionId;
