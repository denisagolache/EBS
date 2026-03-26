package org.ebs.subscription;

import org.ebs.publication.FieldConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FieldPlan {

    private static final String[] OPERATORS_STRING = {"=", "!="};
    private static final String[] OPERATORS_NUM    = {"=", "!=", "<", "<=", ">", ">="};

    private final FieldConfig fieldConfig;
    private final boolean[]   presence;   // presence[i] = campul apare in subscriptia i?
    private final String[]    operators;  // operators[j] = op pentru a j-a aparitie

    public FieldPlan(FieldConfig fieldConfig, int totalCount,
                     double fieldFreq, Double eqFreq) {
        this.fieldConfig = fieldConfig;

        int fieldCount = (int) Math.round(fieldFreq * totalCount);
        fieldCount = Math.max(0, Math.min(fieldCount, totalCount));

        List<Boolean> presenceList = new ArrayList<>(totalCount);
        for (int i = 0; i < fieldCount; i++)       presenceList.add(Boolean.TRUE);
        for (int i = fieldCount; i < totalCount; i++) presenceList.add(Boolean.FALSE);
        Collections.shuffle(presenceList);

        presence = new boolean[totalCount];
        for (int i = 0; i < totalCount; i++) presence[i] = presenceList.get(i);

        operators = buildOperators(fieldCount, eqFreq);
    }

    public boolean isPresent(int subscriptionIndex) {
        return presence[subscriptionIndex];
    }

    public String getOperator(int occurrenceIndex) {
        return operators[occurrenceIndex];
    }

    public FieldConfig getFieldConfig() {
        return fieldConfig;
    }

    private String[] buildOperators(int fieldCount, Double eqFreq) {
        String[] allOps    = getAllOperators();
        String[] nonEqOps  = getNonEqOperators(allOps);

        int eqCount;
        if (eqFreq != null) {
            eqCount = (int) Math.round(eqFreq * fieldCount);
        } else {
            // Distributie uniforma intre toti operatorii
            eqCount = fieldCount / allOps.length;
        }
        eqCount = Math.max(0, Math.min(eqCount, fieldCount));
        int nonEqCount = fieldCount - eqCount;

        List<String> opList = new ArrayList<>(fieldCount);

        // Adauga exact eqCount de "="
        for (int i = 0; i < eqCount; i++) {
            opList.add("=");
        }
        // Distribuie restul uniform intre operatorii != "="
        for (int i = 0; i < nonEqCount; i++) {
            opList.add(nonEqOps[i % nonEqOps.length]);
        }

        Collections.shuffle(opList);
        return opList.toArray(new String[0]);
    }

    private String[] getAllOperators() {
        return switch (fieldConfig.getType()) {
            case ENUM   -> OPERATORS_STRING;
            case DOUBLE -> OPERATORS_NUM;
            case DATE   -> OPERATORS_NUM;
        };
    }

    private String[] getNonEqOperators(String[] allOps) {
        List<String> nonEq = new ArrayList<>();
        for (String op : allOps) {
            if (!op.equals("=")) nonEq.add(op);
        }
        return nonEq.toArray(new String[0]);
    }
}