package bbejeck.spring.model;

import java.text.DecimalFormat;

/**
 * User: Bill Bejeck
 * Date: 11/5/22
 * Time: 12:00 PM
 */
public class LoanAppRollup {

    private double totalLoans;
    private double totalRejected;
    private int numApproved;
    private int numRejected;

    private static final DecimalFormat DF = new DecimalFormat("#,###");

    public LoanAppRollup() {}

    public LoanAppRollup update(LoanApplication loanApplication) {
        if (loanApplication.isApproved()) {
            totalLoans += loanApplication.getAmountRequested();
            numApproved +=1;
        } else {
            totalRejected += loanApplication.getAmountRequested();
            numRejected +=1;
        }
        return this;
    }

    public double getTotalLoans() {
        return totalLoans;
    }

    public double getTotalRejected() {
        return totalRejected;
    }

    public int getNumApproved() {
        return numApproved;
    }

    public int getNumRejected() {
        return numRejected;
    }

    @Override
    public String toString() {
        return "LoanAppRollup{" +
                "totalLoans=" + DF.format(totalLoans) +
                ", totalRejected=" + DF.format(totalRejected) +
                ", numApproved=" + numApproved +
                ", numRejected=" + numRejected +
                '}';
    }
}
