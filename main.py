import concurrent
import json
import sys
import time
from collections.abc import MutableMapping

import pandas as pd
import requests


def change_keys(new, old):
    p = dict((new[key], value) for (key, value) in old.items())
    return p


def flatten_dict(d: MutableMapping, sep: str = '.') -> MutableMapping:
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict


def change_to_decide_format(data):
    final_output = []
    data = pd.read_csv(data)
    data['id'] = data['id'].astype(str)
    account_nos = list(data['id'].unique())
    for account in account_nos:
        statement = data[data['id'] == account].to_json(orient='records')
        statements = json.loads(statement)
        customer_statement = {
            "customer": {
                "id": str(account),
                "email": "test",
                "lastName": "test",
                            "firstName": "test",
                            "userMetadata": {},
                            "phone": "8133703766",
            },
            "bankStatement": {
                "type": "custom",
                "content": {"statement": statements},
            },
        }
        final_output.append(customer_statement)
    return final_output


def get_decide(statement):
    from datetime import datetime
    account_no = statement['customer']['id']

    url = "...."
    r = requests.post(url=url, json=statement)

    if r.status_code == 200:
        r_json = r.json()
        incomeAnalysis = r_json.get("data").get("incomeAnalysis")
        spendAnalysis = r_json.get("data").get("spendAnalysis")
        behaviouralAnalysis = r_json.get("data").get("behaviouralAnalysis")
        cashFlowAnalysis = r_json.get("data").get("cashFlowAnalysis")
        transactionPatternAnalysis = r_json.get(
            "data").get("transactionPatternAnalysis")

        accountSweep = behaviouralAnalysis.get("accountSweep")
        gamblingRate = float(behaviouralAnalysis.get("gamblingRate"))
        inflowOutflowRate = behaviouralAnalysis.get("inflowOutflowRate")

        totalCreditTurnover = cashFlowAnalysis.get("totalCreditTurnover")
        totalDebitTurnover = cashFlowAnalysis.get("totalDebitTurnover")
        noOfTransactingMonths = cashFlowAnalysis.get("noOfTransactingMonths")
        accountActivity = float(cashFlowAnalysis.get("accountActivity"))
        netAverageMonthlyEarnings = cashFlowAnalysis.get(
            "netAverageMonthlyEarnings")
        averageBalance = cashFlowAnalysis.get("averageBalance")
        salaryEarner_ = incomeAnalysis.get("salaryEarner")
        numberSalaryPayments = incomeAnalysis.get("numberSalaryPayments")
        averageOtherIncome = incomeAnalysis.get("averageOtherIncome")
        lastSalaryDate = incomeAnalysis.get("lastSalaryDate")
        lastSalaryDate = (
            lastSalaryDate if lastSalaryDate else "2020-01-01"
        )  # if lastSalaryDate is not found use old date
#         print(lastSalaryDate)

        mostFrequentBalanceRange = transactionPatternAnalysis.get(
            "mostFrequentBalanceRange"
        )

        current_date = datetime.now().date()
        lastSalaryDate = datetime.strptime(lastSalaryDate, "%Y-%m-%d").date()
        salaryAge = str(current_date - lastSalaryDate).split(" ")[0]
        salaryAge = int(salaryAge)

        # noinspection SpellCheckingInspection
        confidenceIntervalOnSalaryDetection = incomeAnalysis.get(
            "confidenceIntervalonSalaryDetection"
        )

        gtbSalaryEarner = False
        highPotentialNonSalary = False

        if (
            salaryEarner_ == "Yes"
            and numberSalaryPayments >= (noOfTransactingMonths - 1)
            and confidenceIntervalOnSalaryDetection > 0.85
            and accountSweep == "No"
            and gamblingRate < 0.25
            and inflowOutflowRate == "Positive Cash Flow"
            and accountActivity > 0.5
            and netAverageMonthlyEarnings > 1000
            and averageBalance > 1000
            and salaryAge <= 35
        ):
            gtbSalaryEarner = True

        if (
            averageOtherIncome > 0
            and accountSweep == "No"
            and gamblingRate < 0.25
            and inflowOutflowRate == "Positive Cash Flow"
            and accountActivity > 0.5
            and averageBalance > (0.5 * netAverageMonthlyEarnings)
            and (mostFrequentBalanceRange != "< 10,000" and mostFrequentBalanceRange != 0)
            and salaryAge <= 35
        ):
            highPotentialNonSalary = True

        output = {
            "status": "success",
            "data": {
                "gtbSalaryEarner": gtbSalaryEarner,
                "highPotentialNonSalary": highPotentialNonSalary,
                "bankStatementSummary": r_json,
                "account_no": account_no
            },
        }

        return output


def remove_and_update_key(dict_data):
    new_keys = {
        'status': 'status',
        'data.highPotentialNonSalary': 'highPotentialNonSalary',
        'data.gtbSalaryEarner': 'gtbSalaryEarner',
        'data.bankStatementSummary.data.behaviouralAnalysis.accountSweep': 'accountSweep',
        'data.bankStatementSummary.data.behaviouralAnalysis.gamblingRate': 'gamblingRate',
        'data.bankStatementSummary.data.behaviouralAnalysis.inflowOutflowRate': 'inflowOutflowRate',
        'data.bankStatementSummary.data.behaviouralAnalysis.loanAmount': 'loanAmount',
        'data.bankStatementSummary.data.behaviouralAnalysis.loanInflowRate': 'loanInflowRate',
        'data.bankStatementSummary.data.behaviouralAnalysis.loanRepaymentInflowRate': 'loanRepaymentInflowRate',
        'data.bankStatementSummary.data.behaviouralAnalysis.loanRepayments': 'loanRepayments',
        'data.bankStatementSummary.data.behaviouralAnalysis.topIncomingTransferAccount': 'topIncomingTransferAccount',
        'data.bankStatementSummary.data.behaviouralAnalysis.topTransferRecipientAccount': 'topTransferRecipientAccount',
        'data.bankStatementSummary.data.cashFlowAnalysis.accountActivity': 'accountActivity',
        'data.bankStatementSummary.data.cashFlowAnalysis.averageBalance': 'averageBalance',
        'data.bankStatementSummary.data.cashFlowAnalysis.averageCredits': 'averageCredits',
        'data.bankStatementSummary.data.cashFlowAnalysis.averageDebits': 'averageDebits',
        'data.bankStatementSummary.data.cashFlowAnalysis.closingBalance': 'closingBalance',
        'data.bankStatementSummary.data.cashFlowAnalysis.firstDay': 'firstDay',
        'data.bankStatementSummary.data.cashFlowAnalysis.lastDay': 'lastDay',
        'data.bankStatementSummary.data.cashFlowAnalysis.monthPeriod': 'monthPeriod',
        'data.bankStatementSummary.data.cashFlowAnalysis.netAverageMonthlyEarnings': 'netAverageMonthlyEarnings',
        'data.bankStatementSummary.data.cashFlowAnalysis.noOfTransactingMonths': 'noOfTransactingMonths',
        'data.bankStatementSummary.data.cashFlowAnalysis.totalCreditTurnover': 'totalCreditTurnover',
        'data.bankStatementSummary.data.cashFlowAnalysis.totalDebitTurnover': 'totalDebitTurnover',
        'data.bankStatementSummary.data.cashFlowAnalysis.yearInStatement': 'yearInStatement',
        'data.bankStatementSummary.data.incomeAnalysis.averageOtherIncome': 'averageOtherIncome',
        'data.bankStatementSummary.data.incomeAnalysis.averageSalary': 'averageSalary',
        'data.bankStatementSummary.data.incomeAnalysis.confidenceIntervalonSalaryDetection': 'confidenceIntervalonSalaryDetection',
        'data.bankStatementSummary.data.incomeAnalysis.expectedSalaryDay': 'expectedSalaryDay',
        'data.bankStatementSummary.data.incomeAnalysis.lastSalaryDate': 'lastSalaryDate',
        'data.bankStatementSummary.data.incomeAnalysis.medianIncome': 'medianIncome',
        'data.bankStatementSummary.data.incomeAnalysis.numberOtherIncomePayments': 'numberOtherIncomePayments',
        'data.bankStatementSummary.data.incomeAnalysis.numberSalaryPayments': 'numberSalaryPayments',
        'data.bankStatementSummary.data.incomeAnalysis.salaryEarner': 'salaryEarner',
        'data.bankStatementSummary.data.incomeAnalysis.salaryFrequency': 'salaryFrequency',
        'data.bankStatementSummary.data.spendAnalysis.airtime': 'airtime',
        'data.bankStatementSummary.data.spendAnalysis.atmWithdrawalsSpend': 'atmWithdrawalsSpend',
        'data.bankStatementSummary.data.spendAnalysis.averageRecurringExpense': 'averageRecurringExpense',
        'data.bankStatementSummary.data.spendAnalysis.bankCharges': 'bankCharges',
        'data.bankStatementSummary.data.spendAnalysis.bills': 'bills',
        'data.bankStatementSummary.data.spendAnalysis.cableTv': 'cableTv',
        'data.bankStatementSummary.data.spendAnalysis.clubsAndBars': 'clubsAndBars',
        'data.bankStatementSummary.data.spendAnalysis.gambling': 'gambling',
        'data.bankStatementSummary.data.spendAnalysis.hasRecurringExpense': 'hasRecurringExpense',
        'data.bankStatementSummary.data.spendAnalysis.internationalTransactionsSpend': 'internationalTransactionsSpend',
        'data.bankStatementSummary.data.spendAnalysis.posSpend': 'posSpend',
        'data.bankStatementSummary.data.spendAnalysis.religiousGiving': 'religiousGiving',
        'data.bankStatementSummary.data.spendAnalysis.spendOnTransfers': 'spendOnTransfers',
        'data.bankStatementSummary.data.spendAnalysis.totalExpenses': 'totalExpenses',
        'data.bankStatementSummary.data.spendAnalysis.ussdTransactions': 'ussdTransactions',
        'data.bankStatementSummary.data.spendAnalysis.utilitiesAndInternet': 'utilitiesAndInternet',
        'data.bankStatementSummary.data.spendAnalysis.webSpend': 'webSpend',
        'data.bankStatementSummary.data.transactionPatternAnalysis.MAWWZeroBalanceInAccount': 'MAWWZeroBalanceInAccount',
        'data.bankStatementSummary.data.transactionPatternAnalysis.NODWBalanceLess5000': 'NODWBalanceLess5000',
        'data.bankStatementSummary.data.transactionPatternAnalysis.highestMAWOCredit.month': 'highestMAWOCredit.month',
        'data.bankStatementSummary.data.transactionPatternAnalysis.highestMAWOCredit.week_of_month': 'highestMAWOCredit.week_of_month',
        'data.bankStatementSummary.data.transactionPatternAnalysis.highestMAWODebit.month': 'highestMAWODebit.month',
        'data.bankStatementSummary.data.transactionPatternAnalysis.highestMAWODebit.week_of_month': 'highestMAWODebit.week_of_month4',
        'data.bankStatementSummary.data.transactionPatternAnalysis.lastDateOfCredit': 'lastDateOfCredit',
        'data.bankStatementSummary.data.transactionPatternAnalysis.lastDateOfDebit': 'lastDateOfDebit',
        'data.bankStatementSummary.data.transactionPatternAnalysis.mostFrequentBalanceRange': 'mostFrequentBalanceRange',
        'data.bankStatementSummary.data.transactionPatternAnalysis.mostFrequentTransactionRange': 'mostFrequentTransactionRange',
        'data.bankStatementSummary.data.transactionPatternAnalysis.recurringExpense': 'recurringExpense',
        'data.bankStatementSummary.data.transactionPatternAnalysis.transactionsBetween100000And500000': 'transactionsBetween100000And500000',
        'data.bankStatementSummary.data.transactionPatternAnalysis.transactionsBetween10000And100000': 'transactionsBetween10000And100000',
        'data.bankStatementSummary.data.transactionPatternAnalysis.transactionsGreater500000': 'transactionsGreater500000',
        'data.bankStatementSummary.data.transactionPatternAnalysis.transactionsLess10000': 'transactionsLess10000',
        'data.account_no': 'account_no'}

    dict_data.pop('data.bankStatementSummary.status', None)
    final_result = change_keys(new_keys, dict_data)
    return final_result


def apply_all_transformations(data):
    try:
        b = flatten_dict(data)
        c = remove_and_update_key(b)
        return c
    except Exception:
        pass


def get_results(csv_file):
    decide_out = change_to_decide_format(csv_file)
    print("Done with converting csv file to decide format")
    print("Get decide results for GTB Bank .......")
    final_res = []
    for i in decide_out:
        res = get_decide(decide_out[0])
        final_res.append(res)

    results = []
    for i in final_res:
        f = apply_all_transformations(i)
        results.append(f)

    print("Task will soon be completed")
    gtb_res = list(filter(None, results))
    gtb_result = pd.DataFrame(gtb_res)
    gtb_result.to_csv("GTB_Decide_results.csv", index=False)
    print("Task has been completed")


if __name__ == "__main__":
    csv_file = sys.argv[1]
    print(csv_file)

    get_results(csv_file)
