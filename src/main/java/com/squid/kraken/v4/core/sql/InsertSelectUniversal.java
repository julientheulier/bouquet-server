/*******************************************************************************
 * Copyright © Squid Solutions, 2016
 *
 * This file is part of Open Bouquet software.
 *  
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation (version 3 of the License).
 *
 * There is a special FOSS exception to the terms and conditions of the 
 * licenses as they are applied to this program. See LICENSE.txt in
 * the directory of this program distribution.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Squid Solutions also offers commercial licenses with additional warranties,
 * professional functionalities or services. If you purchase a commercial
 * license, then it supersedes and replaces any other agreement between
 * you and Squid Solutions (above licenses and LICENSE.txt included).
 * See http://www.squidsolutions.com/EnterpriseBouquet/
 *******************************************************************************/
package com.squid.kraken.v4.core.sql;

import java.util.List;

import com.squid.core.database.model.Column;
import com.squid.core.database.model.Table;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.db.statements.DatabaseInsertInterface;
import com.squid.core.sql.model.Aliaser;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.model.Scope;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.core.sql.render.groupby.IGroupByPiece;
import com.squid.core.sql.statements.InsertSelectStatement;
import com.squid.core.sql.statements.SelectStatement;
import com.squid.kraken.v4.core.analysis.universe.Universe;



/**
 * knows how to insert data from Universe
 *
 */
public class InsertSelectUniversal extends SelectUniversal {

    private DatabaseInsertInterface insert;


    class AdaptativeInsertSelectStatement extends InsertSelectStatement {

        public AdaptativeInsertSelectStatement() {
            super();
        }

        public AdaptativeInsertSelectStatement(Aliaser aliaser) {
            super(aliaser);
        }

        protected IGroupByPiece createGroupByPiece() {
            return getGrouping().createGroupByPiece();
        }

    }


    public InsertSelectUniversal(Universe universe, Table table) throws SQLScopeException {
        super(universe);

        this.insert = new DatabaseInsertInterface(table) {
            protected InsertSelectStatement createStatement(Aliaser aliaser) {
                return new AdaptativeInsertSelectStatement(aliaser);
            }
        };
    }

    public InsertSelectUniversal(Universe universe, Table table, SelectStatement select) throws SQLScopeException {
        super(universe);

        this.insert = new DatabaseInsertInterface(table) {
            protected InsertSelectStatement createStatement(Aliaser aliaser) {
                return new AdaptativeInsertSelectStatement(aliaser);
            }
        };
        this.insert.setInsertFromSelect(select);
        //this.analyzer = new Analyzer(this);
    }

    public InsertSelectUniversal(Universe universe, Table table, IPiece[] values) throws SQLScopeException {
        super(universe);

        this.insert = new DatabaseInsertInterface(table) {
            protected InsertSelectStatement createStatement(Aliaser aliaser) {
                return new AdaptativeInsertSelectStatement(aliaser);
            }
        };
        this.insert.setValues(values);
        //this.analyzer = new Analyzer(this);
    }



    public Analyzer getAnalyzer() {
        return null;//analyzer;
    }

    public void setForceGroupBy(boolean flag) {
        getGrouping().setForceGroupBy(flag);
    }



    /**
     * select the given expression in the current scope
     * @param expression
     * @return
     * @throws SQLScopeException
     * @throws ScopeException
     */
    public ISelectPiece select(ExpressionAST expression) throws SQLScopeException, ScopeException {
        ISelectPiece selectPiece = super.select(expression);
        this.insert.setInsertFromSelect(getStatement());
        return selectPiece;
    }

    public ISelectPiece select(ExpressionAST expression, String name) throws SQLScopeException, ScopeException {
        ISelectPiece selectPiece = super.select(expression, name);
        this.insert.setInsertFromSelect(getStatement());
        return selectPiece;

    }

    public ISelectPiece select(Scope parent, ExpressionAST expression) throws SQLScopeException, ScopeException {
        ISelectPiece selectPiece = super.select(parent, expression);
        this.insert.setInsertFromSelect(getStatement());
        return selectPiece;

    }

    public ISelectPiece select(Scope parent, ExpressionAST expression, String baseName, boolean useAlias, boolean normalizeAlias) throws SQLScopeException, ScopeException {
        ISelectPiece selectPiece = super.select(parent, expression, baseName, useAlias, normalizeAlias);
        this.insert.setInsertFromSelect(getStatement());
        return selectPiece;
    }

    protected String guessExpressionName(ExpressionAST expression) {
        if (expression instanceof ExpressionRef) {
            ExpressionRef ref = (ExpressionRef)expression;
            return ref.getReferenceName();
        }
        if (expression instanceof Compose) {
            Compose compose = (Compose)expression;
            return guessExpressionName(compose.getHead());
        }
        //
        return null;
    }

    public String render() throws RenderingException {
        return this.insert.render(getSkin());
    }

    public void addInsertIntoColumn(List<Column> columns){
        if(columns!=null){
            for(Column column: columns) {
                insert.addInserIntoColumn(column);
            }
        }
    }

    /**
     * internal method we use when rendering a subselect
     * @param skin
     * @return
     * @throws RenderingException
     */
    protected String render(SQLSkin skin) throws RenderingException {
        return this.insert.render(skin);
    }



    @Override
    public String toString() {
        try {
            return render();
        } catch (RenderingException e) {
            return e.toString();
        }
    }

}
