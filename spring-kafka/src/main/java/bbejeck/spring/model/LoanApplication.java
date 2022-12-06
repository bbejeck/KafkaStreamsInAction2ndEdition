package bbejeck.spring.model;

/**
 * User: Bill Bejeck
 * Date: 10/12/22
 * Time: 2:02 PM
 */
public class LoanApplication {

    private String customerId;
    private int creditRating;
    private double amountRequested;
    private double reportedIncome;
    private double reportedDebt;
    private boolean approved;
    private int term;

    public LoanApplication() {
    }

    public LoanApplication(String customerId,
                           int creditRating,
                           double amountRequested,
                           double reportedIncome,
                           double reportedDebt,
                           boolean approved,
                           int term) {
        this.customerId = customerId;
        this.creditRating = creditRating;
        this.amountRequested = amountRequested;
        this.reportedIncome = reportedIncome;
        this.reportedDebt = reportedDebt;
        this.approved = approved;
        this.term = term;
    }

    public String getCustomerId() {
        return customerId;
    }

    public int getCreditRating() {
        return creditRating;
    }

    public double getAmountRequested() {
        return amountRequested;
    }

    public double getReportedIncome() {
        return reportedIncome;
    }

    public double getReportedDebt() {
        return reportedDebt;
    }

    public boolean isApproved() {
        return approved;
    }

    @Override
    public String toString() {
        return "LoanApplication{" +
                "customerId='" + customerId + '\'' +
                ", creditRating=" + creditRating +
                ", amountRequested=" + String.format("%.2f",amountRequested) +
                ", reportedIncome=" + String.format("%.2f",reportedIncome) +
                ", reportedDebt=" + String.format("%.2f",reportedDebt) +
                ", approved=" + approved +
                ", term=" + term +
                '}';
    }

    public int getTerm() {
        return term;
    }


    public static final class Builder {
        private String customerId;
        private int creditRating;
        private double amountRequested;
        private double reportedIncome;
        private double reportedDebt;
        private boolean approved;
        private int term;

        private Builder() {
        }

        private Builder(String customerId,
                        int creditRating,
                        double amountRequested,
                        double reportedIncome,
                        double reportedDebt,
                        boolean approved,
                        int term) {
            this.customerId = customerId;
            this.creditRating = creditRating;
            this.amountRequested = amountRequested;
            this.reportedIncome = reportedIncome;
            this.reportedDebt = reportedDebt;
            this.approved = approved;
            this.term = term;
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static Builder newBuilder(LoanApplication loanApplication) {
            return new Builder(loanApplication.customerId,
                    loanApplication.creditRating,
                    loanApplication.amountRequested,
                    loanApplication.reportedIncome,
                    loanApplication.reportedDebt,
                    loanApplication.approved,
                    loanApplication.term);
        }

        public Builder withCustomerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public Builder withCreditRating(int creditRating) {
            this.creditRating = creditRating;
            return this;
        }

        public Builder withAmountRequested(double amountRequested) {
            this.amountRequested = amountRequested;
            return this;
        }

        public Builder withReportedIncome(double reportedIncome) {
            this.reportedIncome = reportedIncome;
            return this;
        }

        public Builder withReportedDebt(double reportedDebt) {
            this.reportedDebt = reportedDebt;
            return this;
        }

        public Builder withApproved(boolean approved) {
            this.approved = approved;
            return this;
        }

        public Builder withTerm(int term) {
            this.term = term;
            return this;
        }

        public LoanApplication build() {
            return new LoanApplication(customerId, creditRating, amountRequested, reportedIncome, reportedDebt, approved, term);
        }
    }
}
