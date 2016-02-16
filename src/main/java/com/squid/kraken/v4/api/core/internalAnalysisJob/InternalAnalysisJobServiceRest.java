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
package com.squid.kraken.v4.api.core.internalAnalysisJob;

import com.squid.core.database.model.*;
import com.squid.core.database.model.impl.SchemaImpl;
import com.squid.core.database.model.impl.TableImpl;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.reference.Cardinality;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.ExpressionListPiece;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SimpleConstantValuePiece;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.dimension.DimensionServiceBaseImpl;
import com.squid.kraken.v4.api.core.domain.DomainServiceBaseImpl;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobServiceBaseImpl;
import com.squid.kraken.v4.api.core.relation.RelationServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.DynamicDomainContentConstructor;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.engine.query.BaseQuery;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.sql.InsertSelectUniversal;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.core.sql.script.SQLScript;
import com.squid.kraken.v4.model.*;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.export.ExportSourceWriterKafka;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by lrabiet on 18/11/15.
 */

@Produces({MediaType.APPLICATION_JSON})
@Api(value = "internalanalysisjobs", hidden = true, authorizations = {@Authorization(value = "kraken_auth", type = "oauth2")})
public class InternalAnalysisJobServiceRest extends BaseServiceRest {
    private static final Logger logger = LoggerFactory
            .getLogger(InternalAnalysisJobServiceRest.class);

    private final static String PARAM_NAME = "internalJobId";

    private static final boolean SPARK_FLAG = new Boolean(KrakenConfig.getProperty("feature.spark", "false"));

    private InternalAnalysisJobServiceBaseImpl delegate = InternalAnalysisJobServiceBaseImpl
            .getInstance();

    public InternalAnalysisJobServiceRest(AppContext userContext) {
        super(userContext);
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Gets all AnalysisJobs")
    public List<Domain> readAnalysisJobs()
    {
        return delegate.readAll(userContext);
    }

    @POST
    @Path("/")
    @ApiOperation(value = "Gets an AnalysisJobs and save it to an other domain/db")
    public Response transferResult(
            @ApiParam(required = true) Domain newDomain,
            @QueryParam("timeout") Integer timeout,
            @QueryParam("run") Boolean run
            ) {
        return transferAnalysis(newDomain.getOptions().getSourceProjectId(), newDomain.getOptions().getAnalysisJob(), timeout, newDomain.getName(), newDomain.getOptions().getDestSchema(), newDomain.getOptions().getDestProjectId(), run);
    }

    @DELETE
    @Path("{"+PARAM_NAME+"}")
    @ApiOperation(value = "Deletes a domain")
    public boolean delete(@PathParam(PARAM_NAME) String objectId) {
        return DomainServiceBaseImpl.getInstance().delete(userContext,
                new DomainPK(userContext.getCustomerId(), objectId));
    }

    private Response transferAnalysis(
            String sourceProjectId,
            ProjectAnalysisJob job,
            Integer timeout,
            String destDomainName,
            String destSchema,
            String destProjectId,
            Boolean run) {

        Response.ResponseBuilder response = null;
        // Dummy sanity checks
        if (sourceProjectId == null || destDomainName == null || job == null) {
            response = Response.serverError();
            return response.build();
        }

        if (run == null){
            run = true;
        }
        AnalysisJobServiceBaseImpl jobService = AnalysisJobServiceBaseImpl.getInstance();
        ProjectAnalysisJobPK id = new ProjectAnalysisJobPK(
                userContext.getCustomerId(), sourceProjectId, "materializedatasets_"+destDomainName);
        job.setId(id);
        job.setAutoRun(false);
        job.setRedisKey(null);
        job.setCreationTime(System.currentTimeMillis());

        ProjectAnalysisJob job_stored = jobService.store(userContext,job);
        
        if(destProjectId==null || destProjectId==""){ // Go to Spark


            if (SPARK_FLAG) {

                response = Response.ok("Transmitting to Spark...\n");

                if (run) {
                    if (Boolean.valueOf(run)) {
                        byte[] serialized = new byte[1];

                        try {
                            OutputStream out = new OutputStream() {
                                @Override
                                public void write(int b) throws IOException {

                                }
                            };
                            jobService.writeResults(out, userContext, job, 1000,
                                    10000, true, 10000,
                                    0,false, null, null, new ExportSourceWriterKafka());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        /*
                        DataTable results = AnalysisJobServiceBaseImpl.getInstance().readResults(userContext, job_stored.getId(), timeout, false, 1000, 0,false, 1000);
                        serialized = RawMatrix.streamExecutionItemToByteArray(results, -1, 10000);
                        */
                        // serialization done by kafka
                    }

                }
            } else {
                // Set a project or activate spark
                response = Response.serverError();
            }
        }else if (destProjectId.equals(sourceProjectId)) { // Same => Simpler SQL.

            try {

                List<SimpleQuery> queries = delegate.reinject(userContext, job_stored.getId());
                if (logger.isDebugEnabled()) {
                    for (SimpleQuery query : queries)
                        logger.debug(query.toString());
                }
                final ProjectPK project_pk = new ProjectPK(userContext.getCustomerId(), sourceProjectId);
                final Project project = ProjectManager.INSTANCE.getProject(userContext, project_pk);
                Database database = DatabaseServiceImpl.INSTANCE.getDatabase(project);
//
                // Get Schema
                Schema schema = database.findSchema(destSchema);
                if (schema == null){
                    schema = database.findSchema(project.getDbSchemas().get(0));
                    if (schema == null) { // No schema at all?
                        logger.warn("Project is missing a real schema");
                        schema = new SchemaImpl();
                        schema.setName(project.getDbSchemas().get(0));
                        schema.setDatabase(database);
                    }
                }
                // Schema already exists.
                database.getEngine().populateSchema(schema);
                database.getEngine().populateColumns(schema);

                // New table
                Table newTable = database.getFactory().createTable();
                newTable.setName(destDomainName);
                newTable.setSchema(schema);
                schema.addTable(newTable);

                Universe universe = new Universe(userContext, project);
                // Create a createAs Statement.
                SimpleQuery query = queries.get(0);// actually we don't need to have a list here
                SQLScript script = query.generateScript();
                if (script.getSelect()==null) {
                	throw new APIException("Unable to materialize this analysis", userContext.isNoError());
                }
                SelectUniversal select = script.getSelect();
                InsertSelectUniversal insertInto = new InsertSelectUniversal(universe, newTable, select.getStatement());
                SQLScript insertScript = new SQLScript(insertInto);
                // Create the new domain.
                String domainName = newTable.getName();
                String tableRef = newTable.getSchema().isNullSchema() ? newTable.getName() : (newTable.getSchema().getName() + ":" + newTable.getName());
                DomainPK domainPk = new DomainPK(project.getId(), tableRef);
                // TODO use a better expression to express materialize datasets.
                Domain domain = new Domain(domainPk, domainName, new Expression("'" + tableRef + "'"), false);
                DomainOption domainOption = new DomainOption();
                domainOption.setAnalysisJob(job_stored);
                domainOption.setReinjected(true);
                domainOption.setDestProjectId(destProjectId);
                domainOption.setDestSchema(schema.getName());
                domainOption.setSourceProjectId(sourceProjectId);
                domain.setOptions(domainOption);

                if (run) {                    
                    // execute the Script directly
                    Boolean noresult = DatabaseServiceImpl.INSTANCE.execute(query.getDatasource(), insertScript.render());
                    if (!noresult) {
                    	newTable.refresh();
                        DomainServiceBaseImpl.getInstance().store(userContext, domain);
                        // generate dimensions & metrics
                        Space root = universe.S(domain);
                        DynamicDomainContentConstructor cstr = new DynamicDomainContentConstructor(root);
                        for (AxisMapping ax : query.getMapper().getAxisMapping()) {
                        	Column col = newTable.findColumnByName(ax.getPiece().getAlias());
                        	if (col!=null) {
	                        	if (ax.getAxis().getDefinitionSafe().getImageDomain().isInstanceOf(IDomain.OBJECT)) {
	                        		// the dimension is an object - can we create a relation ?
	                        		IDomain image = ax.getAxis().getDefinitionSafe().getImageDomain();
	                        		Object adapter = image.getAdapter(Domain.class);
	                        		if (adapter!=null && adapter instanceof Domain) {
	                        			Domain target = (Domain)adapter;
	                        			Table targetTable = universe.getTable(target);
	                        			if (targetTable.getPrimaryKey()!=null) {
	                        				Index pk = targetTable.getPrimaryKey();
	                        				if (pk.getColumns().size()==1) {
	                        					ExpressionAST join = ExpressionMaker.EQUAL(new ColumnReference(col), new ColumnReference(pk.getColumns().get(0)));
	                        					RelationPK relationPk = new RelationPK(project_pk);
	                        					Relation relation = new Relation(relationPk, domainPk, Cardinality.MANY, target.getId(), Cardinality.ZERO_OR_ONE, domain.getName(), target.getName(), new Expression(join.prettyPrint()));
	                        					RelationServiceBaseImpl.getInstance().store(userContext, relation);
	                        					//
	                    						RelationReference ref = new RelationReference(universe, relation, domain, target);
	                        					cstr.store(cstr.add(cstr.createDimension(ref)));
	                        				}
	                        			}
	                        		}
	                        	} else {
		                        	DimensionIndex index = ax.getDimensionIndex();
		                        	// use the same ID ?
		                        	DimensionPK dimensionPk = new DimensionPK(domainPk, index.getDimension().getId().getDimensionId());
		                        	ColumnReference ref = new ColumnReference(col);
		                        	String formula = ref.prettyPrint();
		                        	Type type = index.getDimension().getType();
		                        	if (type==Type.CATEGORICAL) type=Type.INDEX;// for now disable indexing
		                        	Dimension dimension = new Dimension(dimensionPk, index.getDimensionName(), type, new Expression(formula));
		                        	cstr.store(cstr.add(dimension));
	                        	}
                        	}
                        }
                        // -- create a dimension associated to the metric...
                        for (MeasureMapping mx : query.getMapper().getMeasureMapping()) {
                        	DimensionPK metricPk = new DimensionPK(domainPk, mx.getMapping().getMetric().getId().getMetricId());
                        	Column col = newTable.findColumnByName(mx.getPiece().getAlias());
                        	ColumnReference ref = new ColumnReference(col);
                        	String formula = ref.prettyPrint();
                        	Type type = Type.INDEX;
                        	Dimension metric = new Dimension(metricPk, mx.getMapping().getName(), type, new Expression(formula));
                        	DimensionServiceBaseImpl.getInstance().store(userContext, metric);
                        }
                        response = Response.ok("The domain " + domain.getName() + " has been created ");
                    }
                } else {
                    response = Response.ok(insertScript.render());
                }
            } catch (Exception e) {
            	throw new APIException("failed to materialized dataset: "+e.getLocalizedMessage(), e, userContext.isNoError());
            }

        } else { // Some other Project.


            try {

                final ProjectPK project_pk = new ProjectPK(userContext.getCustomerId(), sourceProjectId);
                ProjectServiceBaseImpl projectService = ProjectServiceBaseImpl.getInstance();
                final Project project = projectService.read(userContext, project_pk, true);
                Database database = DatabaseServiceImpl.INSTANCE.getDatabase(project);
                if(destProjectId==null || destProjectId==""){
                    destProjectId=sourceProjectId;
                }
                final ProjectPK destproject_pk = new ProjectPK(userContext.getCustomerId(), destProjectId);
                Project destproject = null;
                try {
                    destproject = projectService.read(userContext, destproject_pk, true);
                } catch(ObjectNotFoundAPIException e){
                    //TODO Need to create project
                    logger.error("Creating project "+destproject+" is needed");
                    response = Response.serverError();
                    return response.build();
                }

                Schema schema = database.findSchema(destSchema);
                if (schema == null){
                    schema = database.findSchema(project.getDbSchemas().get(0));
                    if (schema == null) { // No schema at all?
                        logger.warn("Project is missing a real schema");
                        schema = new SchemaImpl();
                        schema.setName(project.getDbSchemas().get(0));
                        schema.setDatabase(database);
                    }
                }
                //Schema already exists.
                //database.addSchema("public");
                //database.getEngine().populateSchema(schema);
                //database.getEngine().populateColumns(schema);

                Table newTable = new TableImpl();
                newTable.setName(destDomainName);
                newTable.setSchema(schema);

                String domainName = newTable.getName();
                String tableRef = newTable.getSchema().isNullSchema() ? newTable.getName() : (newTable.getSchema().getName() + ":" + newTable.getName());
                DomainPK domainPk = new DomainPK(project.getId(), tableRef);
                // TODO use a better expression to express materialize datasets.
                Domain domain = new Domain(domainPk, domainName, new Expression("'" + tableRef + "'"), false);
                DomainOption domainOption = new DomainOption();
                domainOption.setAnalysisJob(job_stored);
                domainOption.setReinjected(true);
                domainOption.setDestProjectId(destProjectId);
                domainOption.setDestSchema(schema.getName());
                domainOption.setSourceProjectId(sourceProjectId);
                domain.setOptions(domainOption);
                // Add dep towards the new domain
                for (DomainPK domainPK_dep: job_stored.getDomains()){
                    Domain domain_dep = DomainServiceBaseImpl.getInstance().read(userContext, domainPK_dep);
                    domain_dep.getOptions().addDependency(domain.getId());
                }

                Universe destuniverse = new Universe(userContext, destproject);
                // Get the data.
                DataTable results = AnalysisJobServiceBaseImpl.getInstance().readResults(userContext, job_stored.getId(), timeout, false, 1000, 0,false, 1000);
                if(results==null) {
                    response = Response.serverError();
                    return response.build();
                } else{
                    response = Response.ok(results.getExecutionDate().toString());
                }

                List<Column> columns = new ArrayList<Column>();
                List<DataTable.Col> cols = results.getCols();

                // Convert Col to Column
                for(DataTable.Col col : cols){
                    Column column = new Column();
                    column.setDescription(col.getDefinition());
                    column.setName(col.getName()); // TODO get the correct id not the name (col.getId() is not what i need)
                    column.setNotNullFlag(true);
                    column.setTable(newTable);
                    column.setType(col.getExtendedType());
                    columns.add(column);
                    newTable.addColumn(column);
                    //TODO cannot find/define properly the primary key
                    if(cols.indexOf(col)==1){
                        Index primary = new Index(col.getName());
                        primary.addColumn(column,1);
                        newTable.setPrimaryKey(primary);
                    }
                }

                try {
                    database.addTable(schema.getName(),newTable.getName(),columns);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    response = Response.serverError();
                    return response.build();
                }


                List<IPiece> pieces = new ArrayList<IPiece>();
                int i = 0;
                for (DataTable.Row row : results.getRows()){
                    List<IPiece> subpieces = new ArrayList<IPiece>();
                    i = 0;
                    for(Object s : row.getV()) {
                        IDomain domain_subpiece = columns.get(i).getType().getDomain();
                        subpieces.add(new SimpleConstantValuePiece(s,domain_subpiece));
                        i++;
                    }
                    ExpressionListPiece value = new ExpressionListPiece(subpieces);
                    pieces.add(value);
                }

                IPiece[] pieceA = new IPiece[pieces.size()];
                pieceA = pieces.toArray(pieceA);

                InsertSelectUniversal insertSelectUniversal = new InsertSelectUniversal(destuniverse, newTable, pieceA);
                insertSelectUniversal.addInsertIntoColumn(columns);
                logger.info("For the project " + insertSelectUniversal.getUniverse().getProject().getLName() + "\n" + insertSelectUniversal.render());


                if (run) {
                    if (Boolean.valueOf(run)) {
                        // TODO switch to other project.

                        DomainServiceBaseImpl.getInstance().store(userContext, domain);
                        BaseQuery materializeQuery = new BaseQuery(destuniverse, insertSelectUniversal);
                        Boolean noResult = materializeQuery.execute();
                        if (!noResult) {
                            response = Response.ok("The domain " + domain.getName() + " has been created ");
                            // TODO separate create and insert if needed?
                        } else {
                            response = Response.serverError();
                        }
                    }
                } else {
                    response = Response.ok(insertSelectUniversal.render());
                }

// ERROR: duplicate key value violates unique constraint "'xid'_pkey"
// means the import is too big fir the database to be imported via an insert values.
// You can retry or change method.
            }  catch (SQLScopeException e) {
                e.printStackTrace();
            }  catch (ComputingException e) {
                e.printStackTrace();
            } catch (RenderingException e) {
                e.printStackTrace();
            }
        }



        // return the new domain to be able to do processing on it.
        return response.build();


    }
}
