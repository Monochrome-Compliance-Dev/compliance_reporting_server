const issueDate = record.invoiceIssueDate;
const paidDate = record.paymentDate;

let paymentTime = null;
if (issueDate && paidDate) {
  const issue = new Date(issueDate);
  const paid = new Date(paidDate);
  const diffMs = paid - issue;
  const diffDays = Math.round(diffMs / (1000 * 60 * 60 * 24));
  if (!isNaN(diffDays)) {
    paymentTime = diffDays;
  }
}
