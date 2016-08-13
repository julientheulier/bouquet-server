/*******************************************************************************
 * Copyright © Squid Solutions, 2016
 * <p/>
 * This file is part of Open Bouquet software.
 * <p/>
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation (version 3 of the License).
 * <p/>
 * There is a special FOSS exception to the terms and conditions of the
 * licenses as they are applied to this program. See LICENSE.txt in
 * the directory of this program distribution.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * <p/>
 * Squid Solutions also offers commercial licenses with additional warranties,
 * professional functionalities or services. If you purchase a commercial
 * license, then it supersedes and replaces any other agreement between
 * you and Squid Solutions (above licenses and LICENSE.txt included).
 * See http://www.squidsolutions.com/EnterpriseBouquet/
 *******************************************************************************/
package com.squid.kraken.v4.core.expression.scope;

import java.util.*;

import javax.swing.text.BadLocationException;

import com.squid.core.database.domain.TableDomain;
import com.squid.core.database.model.Table;
import com.squid.core.database.model.TableType;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.ListContentAssistEntry;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.UndefinedExpression;
import com.squid.core.expression.parser.ParseException;
import com.squid.core.expression.parser.TokenMgrError;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.TableReference;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.reference.ParameterReference;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.ExpressionSuggestionItem;
import com.squid.kraken.v4.model.ObjectType;
import com.squid.kraken.v4.model.ValueType;
import org.eclipse.xtext.ide.editor.contentassist.ContentAssistEntry;

public class ExpressionSuggestionHandler {

    private final static String CHARSET_X = "azertyuiopmlkjhgfdsqwxcvbnAZERTYUIOPMLKJHGFDSQWXCVBN1234567890 _@.:|'[]$>";

    private final static String CHARSET_Z = "azertyuiopmlkjhgfdsqwxcvbnAZERTYUIOPMLKJHGFDSQWXCVBN1234567890_";

    private final static String CHARSET = ".$";

    private final static String CLOSING_CHARSET = ")]";

    private final static String OPENING_CHARSET = "([";

    private final static int PROPOSAL_MAX_SIZE = 200;

    private ExpressionScope scope;

    public ExpressionSuggestionHandler(ExpressionScope scope) {
        this.scope = scope;
    }


    /**
     * Get the prefix of a expression.
     *
     * @param text
     * @param offset
     * @return
     * @throws BadLocationException
     */
    private String getPrefix(String text, int offset) {
        if (offset < 0)
            return "";
        int length = 0;
        if (offset >= text.length()) offset = text.length() - 1;
        int level = 0;
        boolean quote = false;
        while (offset >= 0) {
            char c = text.charAt(offset);
            if (c == '\'') {
                quote = !quote;
            } else if (CLOSING_CHARSET.indexOf(c) >= 0) {
                level++;
            } else if (level > 0 && OPENING_CHARSET.indexOf(c) >= 0) {
                level--;
            } else if ((level < 1 && !quote) && CHARSET.indexOf(c) < 0) {
                break;
            }
            length++;
            offset--;
        }

        return text.substring(offset + 1, offset + length + 1);
    }


    public ExpressionSuggestion getSuggestion(String expression, int offset, ValueType valueTypes) {
    	return getSuggestion(expression, offset, null, Collections.singletonList(valueTypes));
    }

    public ExpressionSuggestion getSuggestion(String expression, int offset, Collection<ObjectType> objectTypes, Collection<ValueType> valueTypes) {
        ExpressionSuggestion result = new ExpressionSuggestion();
        boolean isParseSubExpression = false;
        if(expression != null){
            isParseSubExpression = offset > 0 && offset <= expression.length();
        }
        String expressionToParse = isParseSubExpression ? expression.substring(0, offset - 1) : expression;
        Exception error = parseExpression(expressionToParse, result);
        result.setDefinitions(new ArrayList<String>());// patch client app problem if null
        if (error != null && (error.getCause() instanceof ParseException)) {
            ParseException parseError = (ParseException) error.getCause();
            switch (checkIdentifier(parseError.expectedTokenSequences, parseError.tokenImage)) {
                case IDENTIFIER:
                case FUNCTION:
                    updateProposal(result, expressionToParse, objectTypes, valueTypes);
                    break;
                default:
            }
        } else if (error != null && (error.getCause() instanceof TokenMgrError)) {
            updateProposal(result, expressionToParse, objectTypes, valueTypes);
        }
        if (isParseSubExpression) {
            // parse the full expression to get the correct parsing error
            parseExpression(expression, result);
        } else {
        	// just update the proposal
        	updateProposal(result, expressionToParse, objectTypes, valueTypes);
        }
        return result;
    }

    private Exception parseExpression(String expression, ExpressionSuggestion result) {
        try {
            ExpressionAST parsed = expression != "" ? scope.parseExpression(expression) : new UndefinedExpression("");
            ExpressionDiagnostic validation = scope.validateExpression(parsed);
            if (validation == ExpressionDiagnostic.IS_VALID) {
                // just set empty string when the expression is valid
                result.setValidateMessage("");
            } else {
                result.setValidateMessage(validation.getErrorMessage());
            }
        } catch (ScopeException e) {
            result.setValidateMessage(e.getLocalizedMessage() + (e.getCause() != null ? (" caused by " + e.getCause().getLocalizedMessage()) : ""));
            return e;
        }
        // else
        return null;
    }

    private void updateProposal(ExpressionSuggestion exSuggestion, String text, Collection<ObjectType> objectTypes, Collection<ValueType> valueTypes) {
        //
        ExpressionScope actualScope = this.scope;
        String filter = "";
        String prefix = "";
        int filterIndex = text.length();
        //
        // first, get the filtering sequence
        int offset = text.length();
        int x = offset - 1;
        while (x >= 0 && CHARSET_Z.indexOf(text.charAt(x)) >= 0) {
            filter = text.charAt(x) + filter;
            x--;
        }
        filterIndex = x + 1;
        if (x >= 0 && text.charAt(x) == '\'') {// add the first \'
            filter = text.charAt(x) + filter;
            filterIndex = x;
            x--;
        }
        boolean strict_mode = filter.startsWith("'");
        //
        //filter = filter.toUpperCase();
        offset = x + 1;
        prefix = "";
        while (x >= 0 && CHARSET_X.indexOf(text.charAt(x)) >= 0) {
            prefix = text.charAt(x) + prefix;
            x--;
        }
        // look for the context ?
        int dot = prefix.lastIndexOf(".");
        if (dot >= 0) {
            prefix = prefix.substring(0, dot);
            prefix = getPrefix(prefix, offset);
            prefix = prefix.trim();
            // parse the first part of the prefix
            String eval = prefix;//prefix.substring(0, dot);
            //ExpressionParser parser = ExpressionParser.createParser(eval);
            try {
                ExpressionAST expression = scope.parseExpression(eval);//parser.parseExpression(this.context.getScope());
                if (expression.getImageDomain() != IDomain.UNKNOWN) {
                    actualScope = scope.applyExpression(expression);
                }
            } catch (ScopeException e) {
                //ignore
            }
        }
        //
        ArrayList<ExpressionSuggestionItem> proposals = new ArrayList<ExpressionSuggestionItem>();
        //
        {
            List<ExpressionAST> definitions = actualScope.getDefinitions();//buildDefinitionList();
            //
            //System.out.println(prefix);
            for (ExpressionAST expression : definitions) {
                if (expression != null) {
                    //Expression expression = actualScope.createReferringExpression(object);
                    if (expression != null) {
                        String replacement = actualScope.prettyPrint(expression);
                        String upperCaseFilter = filter.toUpperCase();
                        boolean test = strict_mode ? replacement.toUpperCase().startsWith(upperCaseFilter) : replacement.toUpperCase().contains(upperCaseFilter);
                        if (test && replacement != null && !replacement.equals("")) {
                            /*
	                        int cursorPos = replacement.indexOf("???");
	                        if (cursorPos<0) cursorPos = replacement.length();
	                        String displayString = replacement;
	                        proposals.add(displayString);
	                        */
                        	// filter results
                            ExpressionSuggestionItem item = createItem(replacement, expression);
                            if (item.getValueType() != ValueType.ERROR ) {
                                if(valueTypes == null || valueTypes.contains(item.getValueType())) {
                                	if (objectTypes == null || objectTypes.contains(item.getObjectType())) {
                                		if (proposals.size() < PROPOSAL_MAX_SIZE) {
                                			proposals.add(item);
                                		}
                                	}
                                }
                            }
                        }
                        /*
                        String replacement_link = actualScope.prettyPrint(expression);
                        String upperCaseFilter_link = filter.toUpperCase();
                        boolean test_link = strict_mode?replacement.toUpperCase().startsWith(upperCaseFilter):replacement.toUpperCase().contains(upperCaseFilter);
                        if (test_link && replacement_link!=null && replacement_link!="") {
                        	proposals.add(createItemLink(replacement_link, expression));
                        }
                        */
                    }
                }


            }
            try {
            	if (objectTypes == null || objectTypes.contains(ObjectType.FUNCTION)) {
            		//test if it is a function
	                Set<OperatorDefinition> opDefs = this.scope.looseLookup(text);
	                for (OperatorDefinition opDef : opDefs) {
	                    List<List> poly = opDef.getParametersTypes();
	                    ListContentAssistEntry listContentAssistEntry = opDef.getListContentAssistEntry();
	                    if (listContentAssistEntry != null) {
	                        if (listContentAssistEntry.getContentAssistEntries() != null) {
	                            for (ContentAssistEntry contentAssistEntry : listContentAssistEntry.getContentAssistEntries()) {
	                                //TODO this code should disappear when we get to XTEXT
	                                ExpressionSuggestionItem item =
	                                        new ExpressionSuggestionItem(
	                                                opDef.getSymbol() + "(" + contentAssistEntry.getLabel() + ")",
	                                                contentAssistEntry.getDescription(),
	                                                opDef.getSymbol() + "(" + contentAssistEntry.getLabel() + ")",
	                                                opDef.getSymbol() + "(" + contentAssistEntry.getProposal() + ")",
	                                                ObjectType.FUNCTION,
	                                                computeValueTypeFromImage(opDef.computeImageDomain(poly.get(listContentAssistEntry.getContentAssistEntries().indexOf(contentAssistEntry)))),
	                                                0);//computeValueTypeFromImage(opDef.computeImageDomain(type)));
	                                if (item.getValueType() != ValueType.ERROR) {
	                                    if(valueTypes == null || valueTypes.contains(item.getValueType())) {
	                                        if (proposals.size() < PROPOSAL_MAX_SIZE) {
	                                            proposals.add(item);
	                                        }
	                                    }
	                                }
	                            }
	                        }
	                    }
	                }
            	}
            } catch (ScopeException e) {
                //ignoring
            }

        }
        //
        Collections.sort(proposals, new Comparator<ExpressionSuggestionItem>() {

            public int compare(ExpressionSuggestionItem arg0, ExpressionSuggestionItem arg1) {
                if (arg0.getObjectType() == arg1.getObjectType()) {
                    return arg0.getSuggestion().compareTo(arg1.getSuggestion());
                } else {
                    int l0 = getObjectTypePrecedence(arg0.getObjectType());
                    int l1 = getObjectTypePrecedence(arg1.getObjectType());
                    return (l0 == l1) ? arg0.getSuggestion().compareTo(arg1.getSuggestion()) : (l0 < l1 ? -1 : 1);
                }
            }

        });
        exSuggestion.setSuggestions(proposals);
        List<String> legacy = new ArrayList<String>(proposals.size());
        for (ExpressionSuggestionItem item : proposals) {
            legacy.add(item.getSuggestion());
        }
        exSuggestion.setDefinitions(legacy);
        exSuggestion.setFilter(filter);
        exSuggestion.setFilterIndex(filterIndex);
        exSuggestion.setInsertRange(filterIndex, text.length() - 1);
    }

    private ExpressionSuggestionItem createItem(String suggestion, ExpressionAST expr) {
        //TODO handle description escpaially for domain's suggestion.
        if (expr instanceof ExpressionRef && !(expr instanceof ParameterReference)) {
            if (expr instanceof TableReference) {
                return new ExpressionSuggestionItem(
                        ((ExpressionRef) expr).getReferenceName(),
                        ((TableReference) expr).getDescription(),
                        suggestion,
                        computeObjectType(expr),
                        computeValueType(expr));
            } else if (expr instanceof DomainReference) {
                return new ExpressionSuggestionItem(
                        ((ExpressionRef) expr).getReferenceName(),
                        ((DomainReference) expr).getDescription(),
                        suggestion,
                        computeObjectType(expr),
                        computeValueType(expr));
            } else if (expr instanceof RelationReference) {
                return new ExpressionSuggestionItem(
                        ((ExpressionRef) expr).getReferenceName(),
                        ((RelationReference) expr).getDescription(),
                        suggestion,
                        computeObjectType(expr),
                        computeValueType(expr));
            } else if (expr instanceof ColumnReference) {
                return new ExpressionSuggestionItem(
                        ((ExpressionRef) expr).getReferenceName(),
                        ((ColumnReference) expr).getDescription(),
                        suggestion,
                        computeObjectType(expr),
                        computeValueType(expr));
            } else {
                return new ExpressionSuggestionItem(
                        ((ExpressionRef) expr).getReferenceName(),
                        suggestion,
                        computeObjectType(expr),
                        computeValueType(expr));
            }
        } else {
            return new ExpressionSuggestionItem(
                    suggestion,
                    computeObjectType(expr),
                    computeValueType(expr));
        }
    }

        /*
        private ExpressionSuggestionItem createItemLink(String suggestion, ExpressionAST expr) {
            return new ExpressionSuggestionItem(
                    suggestion,
                    computeObjectType(expr),
                    ValueType.LINK);
        }
        */

    private ObjectType computeObjectType(ExpressionAST expr) {
        if (expr instanceof TableReference) {
            return ObjectType.TABLE;
        } else if (expr instanceof ColumnReference) {
            return ObjectType.COLUMN;
        } else if (expr instanceof DomainReference) {
            return ObjectType.DOMAIN;
        } else if (expr instanceof RelationReference) {
            return ObjectType.RELATION;
        } else if (expr instanceof AxisExpression) {
            return ObjectType.DIMENSION;// simplify ?
        } else if (expr instanceof MeasureExpression) {
            return ObjectType.METRIC;// simplify ?
        } else if (expr instanceof SpaceExpression) {
            return ObjectType.DOMAIN;// simplify ?
        } else if (expr instanceof ParameterReference) {
            IDomain image = expr.getImageDomain();
            if (image.isInstanceOf(IDomain.OBJECT)) {
                return ObjectType.DOMAIN;
            } else {
                return ObjectType.EXPRESSION;
            }
        } else return ObjectType.EXPRESSION;
    }

    private ValueType computeValueType(ExpressionAST expr) {
        IDomain image = expr.getImageDomain();
        return computeValueTypeFromImage(image);
    }

    private ValueType computeValueTypeFromImage(IDomain image) {

        if (image.isInstanceOf(IDomain.AGGREGATE))

        {
            return ValueType.AGGREGATE;
        } else if (image.isInstanceOf(IDomain.STRING))

        {
            return ValueType.STRING;
        } else if (image.isInstanceOf(IDomain.NUMERIC))

        {
            return ValueType.NUMERIC;
        } else if (image.isInstanceOf(IDomain.TEMPORAL))

        {
            return ValueType.DATE;
        } else if (image.isInstanceOf(IDomain.CONDITIONAL))

        {
            return ValueType.CONDITION;
        } else if (image.isInstanceOf(DomainDomain.DOMAIN))

        {
            return ValueType.DOMAIN;
        } else if (image.isInstanceOf(TableDomain.DOMAIN))

        {
            Object adapter = image.getAdapter(Table.class);
            if (adapter != null && adapter instanceof Table) {
                Table table = (Table) adapter;
                if (table.getType() == TableType.Table) {
                    return ValueType.TABLE;
                } else {
                    return ValueType.VIEW;
                }
            } else {
                return ValueType.TABLE;
            }
        } else if (image.isInstanceOf(IDomain.OBJECT))

        {
            return ValueType.OBJECT;
        } else if (image.isInstanceOf(IDomain.UNKNOWN))

        {
            return ValueType.ERROR;
        } else return ValueType.OTHER;
    }

    private int getObjectTypePrecedence(ObjectType objectType) {
        switch (objectType) {
            case FOREIGNKEY:
                return -1;
            case EXPRESSION:
            case DIMENSION:
            case METRIC:
                return 0;
            case COLUMN:
                return 1;
            case DOMAIN:
                return 2;
            case TABLE:
                return 3;
            default:
                return 4;
        }
    }

    enum TokenType {
        IDENTIFIER, FUNCTION, DUNNO
    }

    private TokenType checkIdentifier(int[][] expectedTokenSequences, String[] tokens) {
        if (tokens == null) return TokenType.DUNNO;
        for (int[] sequence : expectedTokenSequences) {
            for (int token : sequence) {
                // if expecting an identifier...
                if (tokens[token].equals("<STRING_IDENTIFIER>")) return TokenType.IDENTIFIER;
                // ...or a function (missing starting quote)
                if (tokens[token].equals("\"(\"")) return TokenType.FUNCTION;
            }
        }
        return TokenType.DUNNO;
    }

}
