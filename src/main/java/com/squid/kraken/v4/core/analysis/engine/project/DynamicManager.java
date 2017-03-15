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
package com.squid.kraken.v4.core.analysis.engine.project;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.squid.core.database.domain.TableDomain;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Column;
import com.squid.core.database.model.ForeignKey;
import com.squid.core.database.model.Index;
import com.squid.core.database.model.KeyPair;
import com.squid.core.database.model.Table;
import com.squid.core.database.model.impl.TableImpl;
import com.squid.core.database.statistics.IDatabaseStatistics;
import com.squid.core.database.statistics.PartitionInfo;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.IDomainMetaDomain;
import com.squid.core.domain.associative.AssociativeDomainInformation;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.reference.Cardinality;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.ForeignKeyReference;
import com.squid.core.expression.reference.RelationDirection;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.db.features.IMetadataForeignKeySupport;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.core.analysis.engine.cartography.Cartography;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.CyclicDependencyException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainContent;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.database.impl.DatasourceDefinition;
import com.squid.kraken.v4.core.expression.reference.ColumnDomainReference;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.reference.QueryExpression;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.expression.visitor.ExtractColumns;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.MetricPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ReferencePK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.DimensionDAO;
import com.squid.kraken.v4.persistence.dao.DomainDAO;
import com.squid.kraken.v4.persistence.dao.MetricDAO;
import com.squid.kraken.v4.persistence.dao.RelationDAO;

public class DynamicManager {

	static final Logger logger = LoggerFactory.getLogger(DynamicManager.class);

	public static final boolean DYNAMIC_FLAG = true;// always ON (T1216)
	public static final boolean SPARK_FLAG = new Boolean(KrakenConfig.getProperty("feature.spark", "false"));

	public static final DynamicManager INSTANCE = new DynamicManager();

	private static final DomainDAO domainDAO = ((DomainDAO) DAOFactory.getDAOFactory().getDAO(Domain.class));
	private static final RelationDAO relationDAO = ((RelationDAO) DAOFactory.getDAOFactory().getDAO(Relation.class));
	private static final DimensionDAO dimensionDAO = ((DimensionDAO) DAOFactory.getDAOFactory()
			.getDAO(Dimension.class));
	private static final MetricDAO metricDAO = ((MetricDAO) DAOFactory.getDAOFactory().getDAO(Metric.class));

	/**
	 * generate the list of Domains for the project
	 * 
	 * @param root
	 * @param coverage
	 * @return
	 */
	public List<Domain> loadDomains(Universe root, Map<Table, Domain> coverage) {
		//
		Project project = root.getProject();

		if (SPARK_FLAG) {
			// Discover the projects that are linked to the main project

		}

		List<Domain> domains = domainDAO.findByProject(root.getContext(), project.getId());
		//
		if (DYNAMIC_FLAG) {
			try {
				//
				Collections.sort(domains, new DomainComparator());
				//
				Set<AccessRight> accessRights = project.getAccessRights();
				//
				HashSet<String> checkIDs = new HashSet<String>();// check IDs to
																	// avoid
																	// duplicates
																	// which
																	// will
																	// cause
																	// pain
				List<Domain> scope = new ArrayList<>();
				for (Domain domain : domains) {
					checkIDs.add(domain.getId().getDomainId());
					if (domain.getSubject()!=null && domain.getSubject().getLevel()==0) {// only tables
						try {
							ExpressionAST subject = parseResilient(root, domain, scope);
							if (subject.getImageDomain().isInstanceOf(TableDomain.DOMAIN)) {
								Object adapt = subject.getImageDomain().getAdapter(Table.class);
								if (adapt != null && adapt instanceof Table) {
									coverage.put((Table)adapt,domain);
								}
							}
						} catch (ScopeException e) {
							// ignore only if error is scope exception
							// if other exception, make sure that DB exception
							// will hit the user
						}
					}
					scope.add(domain);
				}
				//
				HashMap<String, Pair<Domain, Table>> checkDuplicate = new HashMap<String, Pair<Domain, Table>>();
				for (Table table : root.getTables()) {
					if (!coverage.containsKey(table)) {
						String domainName = table.getName();// legacy
						// String domainName =
						// normalizeObjectName(table.getName());
						String tableRef = table.getSchema().isNullSchema() ? table.getName()
								: (table.getSchema().getName() + ":" + table.getName());
						String ID = normalizePK(tableRef);
						DomainPK domainPk = new DomainPK(project.getId(), checkUniqueId(ID, checkIDs));
						Domain domain = new Domain(domainPk, domainName, new Expression("'" + tableRef + "'"), true);
						if (table.getDescription()!=null) domain.setDescription(table.getDescription());
						domain.setAccessRights(accessRights);
						AccessRightsUtils.getInstance().setAccessRights(root.getContext(), domain, project);
						domains.add(domain);
						coverage.put(table, domain);// we will need it latter
						checkIDs.add(domainPk.getDomainId());
						// check duplicate table from different schemas
						if (checkDuplicate.containsKey(domainName)) {
							Pair<Domain, Table> duplicate = checkDuplicate.get(domainName);
							if (duplicate != null) {
								// rename the first occurrence
								Table t = duplicate.getSecond();
								Domain d = duplicate.getFirst();
								d.setName(domainName + " (" + t.getSchema().getName() + ")");
								checkDuplicate.put(domainName, null);
							}
							// rename the current occurrence
							domain.setName(domainName + " (" + table.getSchema().getName() + ")");
						} else {
							// add the key
							checkDuplicate.put(domainName, new Pair<Domain, Table>(domain, table));
						}
					}
				}
			} catch (DatabaseServiceException e) {
				logger.error(e.getMessage(), e);
				if (domains.isEmpty()) {
					// there are no domain defined, and the connection failed...
					// better signal it to the user !
					throw e;
				} else {
					// anyway the project cannot be functional without a proper
					// connection...
					// better signal it to the user !
					throw e;
				}
			} catch (ExecutionException e) {
				logger.error(e.getMessage(), e);
			}
		}

		if (SPARK_FLAG) {

		}
		return domains;
	}

	/**
	 * Make sure the ID conforms to the storage layer rules: must match pattern: [a-zA-Z_\-\:0-9]
	 * @param tableRef
	 * @return
	 */
	private String normalizePK(String ID) {
		String normalize = "";
		for (int i=0;i<ID.length();i++) {
			char c = ID.charAt(i);
			if (Character.isLetterOrDigit(c) || c=='_' || c=='-' || c==':') {
				normalize += c;
			} else {
				normalize += "_";
			}
		}
		return normalize;
	}

	class DomainComparator implements Comparator<Domain> {

		@Override
		public int compare(Domain o1, Domain o2) {
			return Integer.compare(o1.getSubject().getLevel(), o2.getSubject().getLevel());
		}
		
	}
	
	
	/**
	 * parse the domain definition / fail back to the internal definition if parsing errors
	 * @param root
	 * @param relation
	 * @param relationScope: the available relations
	 * @return
	 * @throws ScopeException
	 */
	private ExpressionAST parseResilient(Universe root, Domain domain, List<Domain> scope) throws ScopeException {
		try {
			return root.getParser().parse(domain, domain.getSubject().getValue(), scope);
		} catch (ScopeException e) {
			if (domain.getSubject().getInternal()!=null) {
				try {
					ExpressionAST intern = root.getParser().parse(domain, domain.getSubject().getInternal(), scope);
					String value = root.getParser().rewriteExpressionIntern(domain.getSubject().getInternal(), intern);
					domain.getSubject().setValue(value);
					return intern;
				} catch (ScopeException e2) {
					throw e;
				}
			}
			throw e;
		}
	}
	
	protected String checkUniqueId(String ID, HashSet<String> IDs) {
		if (!IDs.contains(ID)) {
			return ID;
		} else {
			int num = 1;
			String dedup = ID + "_" + num;
			while (IDs.contains(dedup)) {
				dedup = ID + "_" + (++num);
			}
			return dedup;
		}
	}

	/**
	 * generate the list of relation for the projects
	 * 
	 * @param root
	 * @param domains
	 * @param coverage
	 * @return
	 */
	public List<Relation> loadRelations(Universe root, List<Domain> domains, Map<Table, Domain> coverage) {
		//
		Project project = root.getProject();
		//
		// populate the relation
		List<Relation> concretes = relationDAO.findByProject(root.getContext(), project.getId());
		// T407:
		Collections.sort(concretes, new RelationComparator());
		//
		if (DYNAMIC_FLAG) {
			try {
				if (root.getDatabase().getSkin()
						.getFeatureSupport(IMetadataForeignKeySupport.ID) == ISkinFeatureSupport.IS_SUPPORTED) {
					return loadDynamicRelations(root, domains, coverage, concretes);
				}
			} catch (DatabaseServiceException e) {
				// unable to build dynamic model for relations
				// but it's ok to return only concrete relations.
			}
		}
		// else
		// fix definitions
		for (Relation concrete : concretes) {
			if (concrete.getJoinExpression() != null) {
				try {
					parseResilient(root, concrete, concretes);
				} catch (ScopeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		//
		return concretes;
	}

	class RelationComparator implements Comparator<Relation> {

		@Override
		public int compare(Relation o1, Relation o2) {
			return Integer.compare(o1.getJoinExpression().getLevel(), o2.getJoinExpression().getLevel());
		}

	}

	/**
	 * parse the relation definition / fail back to the internal definition if
	 * parsing errors
	 * 
	 * @param root
	 * @param relation
	 * @param relationScope:
	 *            the available relations
	 * @return
	 * @throws ScopeException
	 */
	private ExpressionAST parseResilient(Universe root, Relation relation, List<Relation> relationScope)
			throws ScopeException {
		try {
			return root.getParser().parse(relation, relation.getJoinExpression().getValue(), relationScope);
		} catch (ScopeException e) {
			if (relation.getJoinExpression().getInternal() != null) {
				try {
					ExpressionAST intern = root.getParser().parse(relation, relation.getJoinExpression().getInternal(),
							relationScope);
					String value = root.getParser().rewriteExpressionIntern(relation.getJoinExpression().getInternal(),
							intern);
					relation.getJoinExpression().setValue(value);
					return intern;
				} catch (ScopeException e2) {
					throw e;
				}
			}
			throw e;
		}
	}

	class NaturalRelation {
		public boolean first = true;
		public Relation rel;
		public ForeignKey fk;

		public NaturalRelation(Relation rel, ForeignKey fk, boolean first) {
			super();
			this.first = first;
			this.rel = rel;
			this.fk = fk;
		}
	}

	/**
	 * dynamic relation loading: check all existing FK and automatically create
	 * relation if not already concrete
	 * 
	 * @param root
	 * @param domains
	 * @param coverage
	 * @param concretes
	 * @return
	 */
	private List<Relation> loadDynamicRelations(Universe root, List<Domain> domains, Map<Table, Domain> coverage,
			List<Relation> concretes) {
		Project project = root.getProject();
		HashSet<ExpressionAST> dedup = new HashSet<>();// dedup by expression
		HashSet<String> naturals = new HashSet<>();// dedup by ID for naturals
		List<Relation> relations = new ArrayList<Relation>();
		for (Relation concrete : concretes) {
			relations.add(concrete);
			naturals.add(concrete.getOid());
			// check for dedup
			if (concrete.getJoinExpression() != null) {
				try {
					ExpressionAST join = parseResilient(root, concrete, concretes);
					dedup.add(join);
				} catch (ScopeException e) {
					// ignore
				}
			}
		}
		List<Domain> scope = new ArrayList<>();
		for (Domain domain : domains) {
			try {
				if(!domain.getOptions().getReinjected() && !domain.getOptions().getAlink() && domain.getSubject()!=null && domain.getSubject().getLevel()==0) {
					Table table = null;
					ExpressionAST subject = root.getParser().parse(domain,scope);
					if (subject.getImageDomain().isInstanceOf(TableDomain.DOMAIN)) {
						Object adapt = subject.getImageDomain().getAdapter(Table.class);
						if (adapt != null && adapt instanceof Table) {
							table = (Table)adapt;
						}
					}
					if (table!=null) {
						HashMap<String, NaturalRelation> dedupLinks = new HashMap<>();
						for (ForeignKey fk : getForeignKeys(table)) {
							Table targetTable = fk.getPrimaryTable();
							Domain target = coverage.get(targetTable);
							if (target != null) {
								// create a relation ?
								String link = "rel/" + domain.getId().toUUID() + "-" + target.getId().toUUID();// link
																												// from
																												// source
																												// to
																												// target
								String id = link + ":" + fk.getName();
								Relation relation;
								NaturalRelation samesame = null;
								String digest = digest(id);
								if (!naturals.contains(digest)) {
									String leftName = domain.getName();
									String rightName = target.getName();
									samesame = dedupLinks.get(link);
									if (samesame != null) {
										leftName += "[" + fk.getName() + "]";
										rightName += "[" + fk.getName() + "]";
										if (samesame.first) {// it's the first
											samesame.rel.setLeftName(
													samesame.rel.getLeftName() + "[" + samesame.fk.getName() + "]");
											samesame.rel.setRightName(
													samesame.rel.getRightName() + "[" + samesame.fk.getName() + "]");
										}
									}
									RelationPK relationPk = new RelationPK(project.getId(), digest);
									relation = new Relation(relationPk, domain.getId(), Cardinality.MANY,
											target.getId(), Cardinality.ZERO_OR_ONE, leftName, rightName,
											new Expression("'" + fk.getName() + "'"), true);
									AccessRightsUtils.getInstance().setAccessRights(root.getContext(), relation,
											project);
									try {
										ExpressionAST check = root.getParser().parse(relation);
										if (!dedup.contains(check)) {
											relations.add(relation);
										}
									} catch (ScopeException e) {
										// ignore if invalid
									}
									// always handle dedup
									dedupLinks.put(link,
											new NaturalRelation(relation, fk, relation != null && samesame == null));
								}
							}
						}
					}
				} else {
					// Use linked source or origin sources.
					domain.getOptions().getLinkSource();
				}
			} catch (ScopeException e) {
				logger.info(e.getMessage(), e);
			}
		}
		//
		return relations;
	}
	
	// T1771: make the id independent from customer/project ID
	private String genRelationLinkId(Project project, Domain source, Domain target) {
		if (project.getInternalVersion()>=Project.VERSION_1) {
			return "rel/" + source.getOid() + "-" + target.getOid();
		} else {
			return "rel/" + source.getId().toUUID() + "-" + target.getId().toUUID();
		}
	}

	private List<ForeignKey> getForeignKeys(Table table) {
		try {
			return table.getForeignKeys();
		} catch (DatabaseServiceException | ExecutionException e) {
			return Collections.emptyList();
		}
	}

	protected Optional<? extends ExpressionObject<?>> findReference(Universe universe, GenericPK pk) {
		if (pk instanceof DimensionPK) {
			return dimensionDAO.read(universe.getContext(), (DimensionPK) pk);
		} else if (pk instanceof MetricPK) {
			return metricDAO.read(universe.getContext(), (MetricPK) pk);
		} else
			return Optional.absent();
	}

	/**
	 * Populate this Domain content, for both dimensions and metrics. The Space
	 * universe is expected to be associated to the root context - this is not
	 * context specific
	 * 
	 * @param space
	 * @return
	 */
	public DomainContent loadDomainContent(Space space) {
		Universe univ = space.getUniverse();
		Domain domain = space.getDomain();
		//
		// add defined top-level dimensions and record coverage
		List<Dimension> dimensions = dimensionDAO.findByDomain(univ.getContext(), domain.getId());
		List<Metric> metrics = metricDAO.findByDomain(univ.getContext(), domain.getId());
		//
		DomainContent content = new DomainContent(domain, dimensions, metrics);
		//
		loadDomainDynamicContent(space, content);
		//
		return content;
	}


	public void loadDomainDynamicContent(Space space, DomainContent content) {
		try {
	    	Table table = ProjectManager.INSTANCE.getTable(space);
	    	if (table instanceof DynamicTable) {
	    		loadDomainDynamicContentDynamicTable(space, content, (DynamicTable)table);
	    	} else {
	    		loadDomainDynamicContentConcreteTable(space, content, table);
	    	}
        } catch (ScopeException e) {
        	// we should handle the Domain state here == ERROR
        	logger.error("failed to initialize dynamic content for Domain '"+space.getDomain().getName()+"' due to: "+e.getLocalizedMessage(),e);
        }
	}
	
	class DomainContentConcreteState {
		private Space space;
		
		Universe univ;
		Domain domain;
		
		// check if the domain is in legacy mode (i.e. default is to hide dynamic)
		boolean isDomainLegacyMode = true;
		// domainInternalDefautDynamic flag: if legacy mode, hide dynamic object is the default
		boolean domainInternalDefautDynamic = true;
		
		// coverage Set: columns already available through defined dimensions
		HashSet<Column> coverage = new HashSet<Column>();
		HashSet<ExpressionAST> metricCoverage = new HashSet<ExpressionAST>();
		HashSet<Space> neighborhood = new HashSet<Space>();
		HashSet<String> checkName = new HashSet<String>();
		boolean isPeriodDefined = false;
		//
		String prefix = genDomainPrefixID(space.getUniverse().getProject(), domain);
		//
		// evaluate the concrete objects
		HashSet<String> ids = new HashSet<String>();
		// T446: must define the scope incrementally and override the universe
		ArrayList<ExpressionObject<?>> scope = new ArrayList<ExpressionObject<?>>();
		//
		public DomainContentConcreteState(Space space) {
			this.space = space;
			this.univ = space.getUniverse();
			this.domain = space.getDomain();
			//
			// check if the domain is in legacy mode (i.e. default is to hide dynamic)
			this.isDomainLegacyMode = domain.getInternalVersion() == null;
			// domainInternalDefautDynamic flag: if legacy mode, hide dynamic object is the default
			this.domainInternalDefautDynamic = isDomainLegacyMode ? true : false;
			//
			prefix = "dyn_"+space.getDomain().getId().toUUID()+"_dimension:";
		}
		
		// failed List : keep track of failed evaluation, will try again latter
		private List<ExpressionObject<?>> failed = new ArrayList<ExpressionObject<?>>();
		
		/**
		 * @return the failed
		 */
		public List<ExpressionObject<?>> getFailed() {
			return failed;
		}

		public List<ExpressionObject<?>> registerConcreteContent(DomainContent content) {
			// sort by level (0 first, ...)
			List<ExpressionObject<?>> concrete = new ArrayList<ExpressionObject<?>>();
			concrete.addAll(content.getDimensions());
			concrete.addAll(content.getMetrics());
			Collections.sort(concrete, new LevelComparator<ExpressionObject<?>>());
	        for (ExpressionObject<?> object : concrete) {
				if (object.getName()!=null) {
					checkName.add(object.getName());
				}
				if (object instanceof Dimension) {
					// handle Dimension
					Dimension dimension = (Dimension)object;
					if (dimension.getId()!=null) {
						ids.add(dimension.getId().getDimensionId());
						// add also the canonical ID
						if (dimension.getExpression()!=null && dimension.getExpression().getValue()!=null) {
							ids.add(digest(prefix+dimension.getExpression().getValue()));
						}
						// add also the Axis ID
						ids.add(space.A(dimension).getId());
					}
				} else if (object instanceof Metric) {
					// handle Metric
					Metric metric = (Metric)object;
					if (metric.getId()!=null) {
						ids.add(metric.getId().getMetricId());
					}
				}
			}
			return concrete;
		}
		
		public void evalConcreteContent(List<ExpressionObject<?>> concrete, DomainContent incrementalScope) {
			for (ExpressionObject<?> object : concrete) {
				if (object.getName()!=null) {
					checkName.add(object.getName());
				}
				if (object instanceof Dimension) {
					// handle Dimension
					Dimension dimension = (Dimension)object;
					try {
						ExpressionAST expr = parseResilient(univ, domain, dimension, incrementalScope);
						incrementalScope.add(dimension);
						scope.add(object);
						IDomain image = expr.getImageDomain();
						dimension.setImageDomain(image);
						dimension.setValueType(computeValueType(image));
						if (expr instanceof ColumnReference) {
							ColumnReference ref = (ColumnReference)expr;
							if (ref.getColumn()!=null) {
								coverage.add(ref.getColumn());
							}
						} else if (image.isInstanceOf(IDomain.OBJECT)) {
							// it's an sub-domain, we build the space to connect and will dedup for dynamics
							Space path = space.S(expr);
							neighborhood.add(path);
						}
						if (dimension.getType()==Type.CONTINUOUS && image.isInstanceOf(IDomain.TEMPORAL)) {
							isPeriodDefined = true;
						}
				} catch (ScopeException e) {
					// invalid expression, just keep it
					if (logger.isDebugEnabled()) {
						logger.debug(("Invalid Dimension '" + domain.getName() + "'.'" + dimension.getName()
								+ "' definition: " + e.getLocalizedMessage()));
					}
					failed.add(object);
				}
				} else if (object instanceof Metric) {
				// handle Metric
				Metric metric = (Metric) object;
				try {
					if (metric.getId() != null) {
						ids.add(metric.getId().getMetricId());
					}
					if (metric.getExpression() != null) {
						ExpressionAST expr = parseResilient(univ, domain, metric, incrementalScope);
						incrementalScope.add(metric);
						scope.add(object);
						metricCoverage.add(expr);
					}
				} catch (ScopeException e) {
					// invalid expression, just keep it
					if (logger.isDebugEnabled()) {
						logger.debug(("Invalid Metric '" + domain.getName() + "'.'" + metric.getName()
								+ "' definition: " + e.getLocalizedMessage()));
					}
					failed.add(object);
				}
				}
			}
		}
	}
	
	public void loadDomainDynamicContentConcreteTable(Space space, DomainContent content, Table table) {
		Universe univ = space.getUniverse();
		Domain domain = space.getDomain();
		// check if the domain is in legacy mode (i.e. default is to hide
		// dynamic)
		boolean isDomainLegacyMode = domain.getInternalVersion() == null;
		// domainInternalDefautDynamic flag: if legacy mode, hide dynamic object
		// is the default
		boolean domainInternalDefautDynamic = isDomainLegacyMode ? true : false;
		DomainContentConcreteState state = new DomainContentConcreteState(space);
		List<ExpressionObject<?>> concrete = state.registerConcreteContent(content);
		content.setTable(table);// register before eval
		{
			DomainContent incrementalScope = new DomainContent(domain);
			incrementalScope.setTable(table);
			state.evalConcreteContent(concrete, incrementalScope);
		}
		//
		// exclude keys
		HashSet<Column> keys = new HashSet<Column>();
		// filter out the primary-key
		Index pk = table.getPrimaryKey();
		if (pk != null) {
			for (Column col : pk.getColumns()) {
				keys.add(col);
			}
		}
		// filter out the foreign-keys
		try {
			for (ForeignKey fk : space.getTable().getForeignKeys()) {
				for (KeyPair pair : fk.getKeys()) {
					keys.add(pair.getExported());
				}
			}
		} catch (ScopeException | ExecutionException e1) {
			// ignore
		}
		// filter out the relations ?
		ExtractColumns extractor = new ExtractColumns();
		List<Space> subspaces = Collections.emptyList();
		try {
			subspaces = space.S();
		} catch (ScopeException | ComputingException e1) {
			// ignore
		}
		for (Space next : subspaces) {
			Relation relation = next.getRelation();
			try {
				ExpressionAST expr = univ.getParser().parse(relation);
				List<Column> cols = extractor.apply(expr);
				keys.addAll(cols);
			} catch (ScopeException e) {
				// ignore
			}
		}
		//
		// populate dynamic dimensions
		List<RawDImension> periodCandidates = new ArrayList<RawDImension>();
		List<Column> columns = Collections.emptyList();
		try {
			columns = space.getTable().getColumns();
		} catch (ScopeException | ExecutionException e1) {
			// ignore
		}
		for (Column col : columns) {
			if (!keys.contains(col) && !state.coverage.contains(col) && includeColumnAsDimension(col)) {
				ColumnReference ref = new ColumnReference(col);
				String expr = ref.prettyPrint();
				DimensionPK id = new DimensionPK(domain.getId(), digest(state.prefix + expr));
				if (!state.ids.contains(id.getDimensionId())) {
					Type type = Type.INDEX;
					String name = checkName(normalizeObjectName(col.getName()), state.checkName);
					Dimension dim = new Dimension(id, name, type, new Expression(expr), domainInternalDefautDynamic);
					if (col.getDescription()!=null) dim.setDescription(col.getDescription());
					dim.setImageDomain(col.getTypeDomain());
					dim.setValueType(computeValueType(col.getTypeDomain()));
					AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), dim, domain);
					content.add(dim);
					state.checkName.add(name);
					if (col.getTypeDomain().isInstanceOf(IDomain.TEMPORAL) && !state.isPeriodDefined) {
						periodCandidates.add(new RawDImension(col, dim));
					}
				}
			}
		}
		// relation and FK
		for (Space neighbor : subspaces) {
			if (neighbor.length() == 1 // build only direct paths (the facet
										// will populate the others
					&& !state.neighborhood.contains(neighbor)) // dedup if
																// already
																// concrete
																// associated
																// with the same
																// path
			{
				Relation relation = neighbor.getRelation();
				try {
					RelationReference ref = new RelationReference(space.getUniverse(), relation, space.getDomain(),
							neighbor.getDomain());
					if (useRelation(relation, ref)) {
						state.checkName.add(ref.getReferenceName());
						String expr = ref.prettyPrint() + ".$'SELF'";// add the
																		// SELF
																		// parameter
						DimensionPK id = new DimensionPK(domain.getId(), digest(state.prefix + expr));
						if (!state.ids.contains(id.getDimensionId())) {
							String name = ref.getReferenceName();
							if (isDomainLegacyMode) {
								name = checkName(">" + name, state.checkName);
							} else {
								// this is the new naming convention for
								// sub-domains
								name = checkName(name + " > ", state.checkName);
							}
							Dimension dim = new Dimension(id, name, Type.INDEX, new Expression(expr),
									domainInternalDefautDynamic);
							dim.setDescription("relation to "+neighbor.getDomain().getName());
							dim.setValueType(ValueType.OBJECT);
							dim.setImageDomain(ref.getImageDomain());
							AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), dim, domain);
							content.add(dim);
							state.checkName.add(name);
						}
					}
				} catch (ScopeException e) {
					// ignore
				}
			}
		}
		//
		// populate dynamic metrics
		//
		// add count metric
		ExpressionAST count = ExpressionMaker.COUNT();
		if (!state.coverage.contains(count)) {
			Expression expr = new Expression(count.prettyPrint());
			MetricPK metricId = new MetricPK(domain.getId(), digest(state.prefix + expr.getValue()));
			if (!state.ids.contains(metricId.getMetricId())) {// check for
																// natural
																// definition
				String name = "COUNT " + domain.getName();
				name = checkName(name, state.checkName);
				Metric metric = new Metric(metricId, name, expr, domainInternalDefautDynamic);
				metric.setDescription(domain.getName() + " count");
				AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), metric, domain);
				content.add(metric);
				state.checkName.add(name);
			}
		}
		//
		for (Column col : columns) {
			if (col.getTypeDomain().isInstanceOf(IDomain.NUMERIC)) {
				if (!keys.contains(col)) {
					ExpressionAST total = ExpressionMaker.SUM(new ColumnDomainReference(space, col));
					if (!state.coverage.contains(total)) {
						Expression expr = new Expression(total.prettyPrint());
						MetricPK metricId = new MetricPK(domain.getId(), digest(state.prefix + expr.getValue()));
						if (!state.ids.contains(metricId.getMetricId())) {// check
																			// for
																			// natural
																			// definition
							String name = "SUM " + normalizeObjectName(col.getName());
							name = checkName(name, state.checkName);
							Metric metric = new Metric(metricId, name, expr, domainInternalDefautDynamic);
							if (col.getDescription()!=null) metric.setDescription(col.getDescription());
							AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), metric, domain);
							content.add(metric);
							state.checkName.add(name);
						}
					}
				}
			}
		}
		//
		// try to recover failed ones
		for (ExpressionObject<?> object : state.getFailed()) {
			try {
				if (object.getExpression() != null) {
					if (object instanceof Dimension) {
						// handle Dimension
						Dimension dimension = (Dimension) object;
						ExpressionAST expr = parseResilient(univ, domain, dimension, content);
						content.add((Dimension) object);
						IDomain image = expr.getImageDomain();
						dimension.setImageDomain(image);
						dimension.setValueType(computeValueType(image));
					} else if (object instanceof Metric) {
						// handle Metric
						Metric metric = (Metric) object;
						ExpressionAST expr = parseResilient(univ, domain, metric, content);
						content.add((Metric) object);
						IDomain image = expr.getImageDomain();
						metric.setImageDomain(image);
						metric.setValueType(computeValueType(image));
					}
				}
			} catch (ScopeException | CyclicDependencyException e) {
				// set as permanent error
			}
		}
		//
		// select a Period if needed
		boolean isFact = isFactDomain(univ.getContext(), domain.getId());
		boolean needPeriod = !state.isPeriodDefined // if already defined,
													// that's fine
				&& isFact // it must be a fact table, if not there is a good
							// chance to pollute
				&& content.getMetrics().size() > 1; // and we want at least a
													// metric different than
													// COUNT()
		// select the period
		if (needPeriod && !periodCandidates.isEmpty()) {
			DimensionPeriodSelector selector = new DimensionPeriodSelector(space.getUniverse());
			RawDImension candidate = selector.selectPeriod(periodCandidates);
			if (candidate != null) {
				candidate.dim.setType(Type.CONTINUOUS);
			}
		}
	}
	
	public void loadDomainDynamicContentDynamicTable(Space space, DomainContent content, DynamicTable table) throws ScopeException {
		Universe univ = space.getUniverse();
		Domain domain = space.getDomain();
		//
		DomainContentConcreteState state = new DomainContentConcreteState(space);
		//
		// register the concrete objects
		// ... to avoid creating duplicates dimensions/metrics
		List<ExpressionObject<?>> concrete = state.registerConcreteContent(content);
		//
		content.setTable(table);
		DomainContent incrementalScope = new DomainContent(domain);
		incrementalScope.setTable(table);
		//
		// evaluate the query
		QueryExpression query = table.getLineage();
		if (query.getFacets().isEmpty() && query.getMetrics().isEmpty()) {
			// not that fun, just copy the source domain dimension
			throw new ScopeException("Domain inheritence not yet supported");
		} else {
			// let's populate the table
			//
			// find a cool table name
			{
				String name = DynamicManager.INSTANCE.digest(query.prettyPrint());
				table.setName(name);
			}
			//
			// ok, let's start with the facets
			SQLSkin skin = univ.getDatabase().getSkin();// need that to compute extendedTypes
			Index primaryKey = new Index("PK");
			int pos = 0;
			List<RawDImension> periodCandidates = new ArrayList<RawDImension>();
			for (ExpressionAST facet : query.getFacets()) {
				//
				IDomain image = facet.getImageDomain();
				if (image.isInstanceOf(IDomain.OBJECT)) {
					Domain target = (Domain)(image.getAdapter(Domain.class));
					//ExpressionAST leftDomain = new ParameterReference("LEFT", space.getImageDomain());
					//ExpressionAST rightDomain = new ParameterReference("RIGHT", image);
					// need to find the key
					ExpressionFunctor functor = extractRelationSafe(univ, facet);
					ExpressionFunctor join = functor;
					if (functor!=null) {
						IDomain compare = query.getSourceDomain();
						for (ExpressionAST variable : functor.getVariables()) {
							IDomain source = variable.getSourceDomain();
							if (source.isInstanceOf(compare)) {
								DynamicColumn col = createDynamicColumn(table, variable, skin);
								col.setTable(table);
								table.addColumn(col);
								primaryKey.addColumn(col, pos++);
								//
								ExpressionAST rebind = ExpressionMaker.COMPOSE(new DomainReference(space), new ColumnDomainReference(space, col));
								join = join.replace(variable, rebind);
							}
						}
					}
					// create the relation to target
					if (join!=functor) {
						String idrel = "rel/" + domain.getId().toUUID() + "-" + target.getId().toUUID() + ":" + facet.prettyPrint();
						String digest = digest(idrel);
						RelationPK relationPk = new RelationPK(univ.getProject().getId(), digest);
						Relation relation =
								new Relation(relationPk,
										domain.getId(),
										Cardinality.MANY,
										target.getId(),
										Cardinality.ZERO_OR_ONE,
										domain.getName(),
										target.getName(),
										new Expression(join.getDefinition().prettyPrint()), true);
						AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), relation, univ.getProject());
						// add the relation locally
						content.add(relation);
						incrementalScope.add(relation);
						//
						try {
							RelationReference ref = new RelationReference(univ, relation, domain, target);
							if (true) {//(useRelation(relation, ref)) {
								state.checkName.add(ref.getReferenceName());
								String expr = ref.prettyPrint()+".$'SELF'";// add the SELF parameter
								DimensionPK id = new DimensionPK(domain.getId(), digest(state.prefix+expr));
								if (!state.ids.contains(id.getDimensionId())) {
									String name = ref.getReferenceName();
									name = checkName(">"+name,state.checkName);
									Dimension dim = new Dimension(id, name, Type.INDEX, new Expression(expr), false);
									dim.setValueType(ValueType.OBJECT);
									dim.setImageDomain(ref.getImageDomain());
									AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), dim, domain);
									content.add(dim);
									incrementalScope.add(dim);
									state.scope.add(dim);
									state.checkName.add(name);
								}
							}
						} catch (ScopeException e) {
							// ignore
						}
					} else {
						// register an invalid dimension
						
					}
				} else {
					DynamicColumn col = createDynamicColumn(table, facet, skin);
					col.setTable(table);
					table.addColumn(col);
					primaryKey.addColumn(col, pos++);
					//
					ColumnReference ref = new ColumnReference(col);
					String expr = ref.prettyPrint();
					String idgen = facet.prettyPrint();// use the facet definition
					DimensionPK id = new DimensionPK(domain.getId(), digest(state.prefix+idgen));
					if (!state.ids.contains(id.getDimensionId())) {
						Type type = Type.INDEX;
						String name = checkName(getIdentifierName(facet),state.checkName);
						Dimension dim = new Dimension(id, name, type, new Expression(expr),false);
						dim.setImageDomain(col.getTypeDomain());
						dim.setValueType(computeValueType(col.getTypeDomain()));
						AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), dim, domain);
						content.add(dim);
						incrementalScope.add(dim);
						state.scope.add(dim);
						state.checkName.add(name);
						if (col.getTypeDomain().isInstanceOf(IDomain.TEMPORAL)) {
							periodCandidates.add(new RawDImension(col, dim));
						}
					}
				}
			}
			//
			if (!primaryKey.getColumns().isEmpty()) {
				table.setPrimaryKey(primaryKey);
			}
			//
			for (ExpressionAST metric : query.getMetrics()) {
				DynamicColumn col = createDynamicColumn(table, metric, skin);
				col.setTable(table);
				table.addColumn(col);
				//
				ColumnReference ref = new ColumnReference(col);
				String expr = ref.prettyPrint();
				String idgen = metric.prettyPrint();// use the facet definition
				DimensionPK id = new DimensionPK(domain.getId(), digest(state.prefix+idgen));
				if (!state.ids.contains(id.getDimensionId())) {
					Type type = Type.INDEX;
					String name = checkName(normalizeObjectName(col.getName()),state.checkName);
					Dimension dim = new Dimension(id, name, type, new Expression(expr),false);
					dim.setImageDomain(col.getTypeDomain());
					dim.setValueType(computeValueType(col.getTypeDomain()));
					AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), dim, domain);
					content.add(dim);
					incrementalScope.add(dim);
					state.scope.add(dim);
					state.checkName.add(name);
					if (col.getTypeDomain().isInstanceOf(IDomain.TEMPORAL)) {
						periodCandidates.add(new RawDImension(col, dim));
					}
				}
				//
				// associative ?
				IDomain image = metric.getImageDomain();
				OperatorDefinition op = AssociativeDomainInformation.getAssociativeOperator(image);
				if (op!=null) {
					ExpressionAST m = ExpressionMaker.op(op, ref);
					String expr2 = m.prettyPrint();
					MetricPK id2 = new MetricPK(domain.getId(), digest(state.prefix+expr2));
					if (!state.ids.contains(id2)) {
						String name2 = checkName(op.getName()+" "+normalizeObjectName(col.getName()),state.checkName);
						Metric metric2 = new Metric(id2, name2, new Expression(expr2),false);
						AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), metric2, domain);
						content.add(metric2);
						incrementalScope.add(metric2);
						state.scope.add(metric2);
						state.checkName.add(name2);
					}
				}
			}
			//
			// now we can evaluate the concrete objects
			state.evalConcreteContent(concrete, incrementalScope);
			// select the period
			if (!periodCandidates.isEmpty() && !state.isPeriodDefined) {
				DimensionPeriodSelector selector = new DimensionPeriodSelector(space.getUniverse());
				RawDImension candidate = selector.selectPeriod(periodCandidates);
				if (candidate!=null) {
					candidate.dim.setType(Type.CONTINUOUS);
				}
			}
		}
	}
	
	private DynamicColumn createDynamicColumn(Table table, ExpressionAST definition, SQLSkin skin) throws ScopeException {
		String internal = normalizeColumnName(table,getExpressionName(definition));
		DynamicColumn col = new DynamicColumn();
		col.setName(internal);
		col.setDescription(definition.prettyPrint());
		col.setLineage(definition);// will be use to compute the column value
		// set Type - make sure we skip the aggregate part
		ExtendedType ext = definition.computeType(skin);
		// handle the imageDomain to avoid metaInformation
		IDomain image = definition.getImageDomain();
		if (image.isInstanceOf(IDomain.META)) {
			image = ((IDomainMetaDomain)image).getSubdomain();
			col.setType(new ExtendedType(image, ext));
		} else {
			col.setType(ext);
		}
		return col;
	}
	
	private String getExpressionName(ExpressionAST expr) {
		if (expr.getName()!=null) {
			// use the defined name:
			return expr.getName();
		} else if (expr instanceof ExpressionRef) {
			ExpressionRef ref = (ExpressionRef)expr;
			return ref.getReferenceName();
		} else {
			// assume it is a formula
			String internal = expr.prettyPrint();
			return internal;
		}
	}

	
	private String getIdentifierName(ExpressionAST expr) {
		if (expr.getName()!=null) {
			// use the defined name, it's already a valid identifier so no need to change
			return expr.getName();
		} else if (expr instanceof ExpressionRef) {
			// use the defined name, it's already a valid identifier so no need to change
			ExpressionRef ref = (ExpressionRef)expr;
			return ref.getReferenceName();
		} else {
			// assume it is a formula
			String internal = expr.prettyPrint();
			// need to clean up
			return normalizeIdentifierName(internal);
		}
	}
	
	/**
	 * make sure the name is OK for defining an identifier
	 * @param name
	 * @return
	 */
	private String normalizeIdentifierName(String name) {
		return name
				.replaceAll("[(),.]", " ") // keep some special chars as separators
				.trim() // remove unnecessary separators
				.replaceAll("[^ a-zA-Z_0-9]", "") // remove anything which is not supported
				.replace(' ','_'); // underscore the spaces
	}

	/**
	 * make sure the name is OK for defining a SQL column
	 * @param table is the target table: needed to avoid collision
	 * @param name
	 * @return
	 * @throws ScopeException 
	 */
	private String normalizeColumnName(Table table, String name) throws ScopeException {
		String norm = normalizeIdentifierName(name);
		if (norm.length()>30) {
			// need to truncate
			norm = norm.substring(0,30);
		}
		int count = 0;
		try {
			String candidate = norm;
			while (table.findColumnByName(candidate)!=null) {
				count++;
				String alias = ""+count;
				if (norm.length()+alias.length()>30) {
					if (alias.length()>=30) {
						// that will never happen right?
						throw new ScopeException("unable to normalize column '"+name+"' - well played guy!"); 
					}
					candidate = norm.substring(0,30-alias.length());
					candidate += alias;
				}
			}
			return candidate;
		} catch (ExecutionException e) {
			return norm;
		}
	}
	
	private ExpressionFunctor extractRelationSafe(Universe univ, ExpressionAST expr) {
		try {
			return extractRelation(univ, expr);
		} catch (ScopeException | SQLScopeException e) {
			return null;
		}
	}
	
	private ExpressionFunctor extractRelation(Universe univ, ExpressionAST expr) throws ScopeException, SQLScopeException {
		if (expr instanceof AxisExpression) {
			AxisExpression ref = (AxisExpression)expr;
			Axis axis = ref.getAxis();
			return extractRelation(univ, axis.getDefinitionSafe());
		}
		if (expr instanceof RelationReference) {
			RelationReference ref = (RelationReference)expr;
			return getExportedKey(univ, ref);
		} if (expr instanceof Compose) {
			Compose compose = (Compose)expr;
			ExpressionFunctor functor = extractRelation(univ, compose.getHead());
			for (ExpressionAST variable : functor.getVariables()) {
				// rebind
				Compose rebind = new Compose(compose.getTail(), variable);
				functor = functor.replace(variable, rebind);
			}
			return functor;// relink exported key
		} else {
			return null;
		}
	}
	
	private ExpressionFunctor getExportedKey(Universe universe, RelationReference ref) throws ScopeException, SQLScopeException {
		Relation rel = ref.getRelation();
		Domain left = ref.getLeftDomain();
		Domain right = ref.getRightDomain();
		if (left==null || right==null) {
			throw new ScopeException("Relation does not define Domains: "+ref.getReference().toString());
		}
		ExpressionAST join = universe.getParser().parse(rel);
		if (join instanceof ForeignKeyReference) {
			// in that case we have to translate the FK into a regular expression to transform
			ForeignKey fk = ((ForeignKeyReference)join).getForeignKey();
			if (fk.getKeys().isEmpty()) {
				throw new SQLScopeException("Relation not supported (no valid key): "+ref.getReference().toString());
			}
			List<ExpressionAST> joins = new ArrayList<>();
			for (KeyPair key : fk.getKeys()) {
				ExpressionAST exported = 
						ExpressionMaker.COMPOSE(new DomainReference(universe, left), new ColumnReference(key.getExported()));
				ExpressionAST primary = 
						ExpressionMaker.COMPOSE(new DomainReference(universe, right), new ColumnReference(key.getPrimary()));
				joins.add(ExpressionMaker.EQUAL(exported, primary));
			}
			ExpressionAST explicitJoin = null;
			if (joins.isEmpty()) {
				throw new SQLScopeException("Relation not supported (no valid key): "+ref.getReference().toString());
			} else if (joins.size()==1) {
				explicitJoin = joins.get(0);
			} else {
				explicitJoin = ExpressionMaker.AND(joins);
			}
			ExpressionFunctor functor = new ExpressionFunctor(explicitJoin);
			return functor;
		} else {
			return new ExpressionFunctor(join);
		}
	}
	
	class ExpressionPair {
		
		public ExpressionAST left;
		public ExpressionAST right;
		
		/**
		 * reorder the keys given the direction
		 * @param exported
		 * @param primary
		 * @param direction
		 * @throws ScopeException
		 */
		public ExpressionPair(ExpressionAST exported, ExpressionAST primary, RelationDirection direction) throws ScopeException {
			switch (direction) {
			case LEFT_TO_RIGHT:
				left = exported;
				right = primary;
				break;
			case RIGHT_TO_LEFT:
				left = primary;
				right = exported;
				break;
			default:
				throw new ScopeException("invalid relation");
			}
		}
	}
	

	/**
	 * generate the Table mapping for that query => imagine that the query is materialized...
	 * @return
	 */
	public Table genQueryTableMapping(Universe universe, QueryExpression query) {
		Table materialized = new TableImpl();
		//
		// find a cool table name
		{
			String name = DynamicManager.INSTANCE.digest(query.prettyPrint());
			materialized.setName(name);
		}
		//
		// ok, let's start with the facets
		SQLSkin skin = universe.getDatabase().getSkin();// need that to compute extendedTypes
		Index primaryKey = new Index("PK");
		int pos = 0;
		for (ExpressionAST facet : query.getFacets()) {
			String internal = normalizeObjectName(facet.prettyPrint());
			internal = internal.replaceAll("'", "");
			internal = internal.replaceAll("\\(", "_");
			internal = internal.replaceAll("\\)", "");
			//
			IDomain domain = facet.getImageDomain();
			if (domain.isInstanceOf(IDomain.OBJECT)) {
				// need to find the key
			} else {
				Column col = new Column();
				col.setName(internal);
				col.setDescription(facet.prettyPrint());
				col.setType(facet.computeType(skin));
				col.setTable(materialized);
				materialized.addColumn(col);
				primaryKey.addColumn(col, pos++);
			}
		}
		//
		if (!primaryKey.getColumns().isEmpty()) {
			materialized.setPrimaryKey(primaryKey);
		}
		//
		for (ExpressionAST metric : query.getMetrics()) {
			String internal = normalizeObjectName(metric.prettyPrint());
			internal = internal.replaceAll("'", "");
			internal = internal.replaceAll("\\(", "_");
			internal = internal.replaceAll("\\)", "");
			//
			Column col = new Column();
			col.setName(internal);
			col.setDescription(metric.prettyPrint());
			ExtendedType ext = metric.computeType(skin);
			// handle the imageDomain to avoid metaInformation
			IDomain image = metric.getImageDomain();
			if (image.isInstanceOf(IDomain.META)) {
				image = ((IDomainMetaDomain)image).getSubdomain();
			}
			col.setType(new ExtendedType(image, ext));
			col.setTable(materialized);
			materialized.addColumn(col);
		}
		//
		return materialized;
	}
	
	// T1771: generate a prefix ID independent from the customer/project ID
	private String genDomainPrefixID(Project project, Domain domain) {
		if (project.getInternalVersion()>=Project.VERSION_1) {
			return "dyn_" + domain.getOid() + "_dimension:";
		} else {
			return "dyn_" + domain.getId().toUUID() + "_dimension:";
		}
	}

	private boolean includeColumnAsDimension(Column col) {
		IDomain image = col.getTypeDomain();
		if (image.isInstanceOf(IDomain.TEMPORAL) || image.isInstanceOf(IDomain.STRING)
				|| image.isInstanceOf(IDomain.CONDITIONAL)) {
			return true;
		} else if (image.isInstanceOf(IDomain.NUMERIC)) {
			return col.getType().isInteger();
		} else {
			return false;
		}
	}

	private String checkName(String nameToCheck, Set<String> existingNames) {
		if (existingNames.contains(nameToCheck)) {
			String newName = nameToCheck + " (copy)";
			int index = 0;
			while (existingNames.contains(newName)) {
				newName = nameToCheck + " (copy " + (++index) + ")";
			}
			return newName;
		} else {
			return nameToCheck;
		}
	}

	private ExpressionAST parseResilient(Universe root, Domain domain, Dimension dimension, DomainContent scope) throws ScopeException {
		try {
			if (dimension.getExpression().getReferences() != null) {
				ArrayList<ExpressionObject<?>> externalRefs = new ArrayList<>();
				for (Object safeCasting : dimension.getExpression().getReferences()) {
					if (safeCasting instanceof ReferencePK<?>) {
						ReferencePK<?> unwrap = (ReferencePK<?>) safeCasting;
						GenericPK ref = unwrap.getReference();
						if (!ref.getParent().equals(domain.getId())) {// not for internal ref
							Optional<? extends ExpressionObject<?>> value = findReference(root, ref);
							if (value.isPresent()) {
								externalRefs.add(value.get());
							}
						}
					}
				}
				if (!externalRefs.isEmpty()) {
					scope = new DomainContent(scope);
					scope.addAll(externalRefs);
				}
			}
			return root.getParser().parse(domain, dimension, dimension.getExpression().getValue(), scope);
		} catch (CyclicDependencyException e) {
			throw new ScopeException(
					"Unable to parse Dimension " + dimension + " because of unresolved cyclic dependency", e);
		} catch (ScopeException e) {
			if (dimension.getExpression().getInternal() != null) {
				try {
					ExpressionAST intern = root.getParser().parse(domain, dimension,
							dimension.getExpression().getInternal(), scope);
					String value = root.getParser().rewriteExpressionIntern(dimension.getExpression().getInternal(),
							intern);
					dimension.getExpression().setValue(value);
					return intern;
				} catch (ScopeException e2) {
					throw e;
				}
			}
			throw e;
		}
	}
	
	private ExpressionAST parseResilient(Universe root, Domain domain, Metric metric, DomainContent scope) throws ScopeException {
		try {
			if (metric.getExpression().getReferences() != null) {
				ArrayList<ExpressionObject<?>> externalRefs = new ArrayList<>();
				for (Object safeCasting : metric.getExpression().getReferences()) {
					if (safeCasting instanceof ReferencePK<?>) {
						ReferencePK<?> unwrap = (ReferencePK<?>) safeCasting;
						GenericPK ref = unwrap.getReference();
						if (!ref.getParent().equals(domain.getId())) {// not for internal ref
							Optional<? extends ExpressionObject<?>> value = findReference(root, ref);
							if (value.isPresent()) {
								externalRefs.add(value.get());
							}
						}
					}
				}
				if (!externalRefs.isEmpty()) {
					scope = new DomainContent(scope);
					scope.addAll(externalRefs);
				}
			}
			return root.getParser().parse(domain, metric, metric.getExpression().getValue(), scope);
		} catch (ScopeException e) {
			if (metric.getExpression().getInternal() != null) {
				try {
					ExpressionAST intern = root.getParser().parse(domain, metric, metric.getExpression().getInternal(),
							scope);
					String value = root.getParser().rewriteExpressionIntern(metric.getExpression().getInternal(),
							intern);
					metric.getExpression().setValue(value);
					return intern;
				} catch (ScopeException e2) {
					throw e;
				}
			}
			throw e;
		}
	}

	class LevelComparator<X extends ExpressionObject<?>> implements Comparator<X> {

		@Override
		public int compare(X o1, X o2) {
			return Integer.compare(o1.getExpression().getLevel(), o2.getExpression().getLevel());
		}

	}

	/**
	 * check if needs to follow this relation automatically when defining
	 * dynamic sub-domains
	 * 
	 * @param rel
	 * @param ref
	 * @return
	 */
	private boolean useRelation(Relation rel, RelationReference ref) {
		if (rel.getLeftCardinality() != Cardinality.MANY && rel.getRightCardinality() != Cardinality.MANY) {
			return ref.getDirection() == RelationDirection.LEFT_TO_RIGHT;
		} else if (rel.getLeftCardinality() != Cardinality.MANY) {
			return ref.getDirection() == RelationDirection.RIGHT_TO_LEFT;
		} else if (rel.getRightCardinality() != Cardinality.MANY) {
			return ref.getDirection() == RelationDirection.LEFT_TO_RIGHT;
		} else {
			return false;
		}
	}

	/**
	 * simple wrapper to keep the original column reference for dynamic columns
	 * 
	 * @author sergefantino
	 *
	 */
	class RawDImension {

		public Column col = null;
		public Dimension dim = null;

		public RawDImension(Column col, Dimension dim) {
			super();
			this.col = col;
			this.dim = dim;
		}

	}

	/**
	 * optimize the selection of a period dimension: - select a partition key if
	 * exists - select a date - select a timestamp
	 * 
	 * If the selected period is a timestamp, convert it to a date first
	 * 
	 * @author sergefantino
	 *
	 */
	class DimensionPeriodSelector {

		private IDatabaseStatistics stats;

		public DimensionPeriodSelector(Universe universe) {
			DatasourceDefinition ds = DatabaseServiceImpl.INSTANCE.getDatasourceDefinition(universe.getProject());
			stats = ds.getDBManager().getStatistics();
		}

		public RawDImension selectPeriod(List<RawDImension> periodCandidates) {
			RawDImension select = null;
			int score = 0;
			for (RawDImension candidate : periodCandidates) {
				if (select == null) {
					select = candidate;
					score = computePeriodScore(candidate);
				} else {
					int challenge = computePeriodScore(candidate);
					if (challenge > score) {
						select = candidate;
						score = challenge;
					}
				}
			}
			//
			if (select != null) {
				ExtendedType ext = select.col.getType();
				if (ext.getDomain().isInstanceOf(IDomain.TIMESTAMP)) {
					// convert to date
					ColumnReference ref = new ColumnReference(select.col);
					String expr = "TO_DATE(" + ref.prettyPrint() + ")";
					select.dim.setExpression(new Expression(expr));
				}
			}
			//
			return select;
		}

		private int computePeriodScore(RawDImension candidate) {
			if (isPartitionKey(candidate.col)) {
				return 100;
			} else {
				ExtendedType ext = candidate.col.getType();
				if (ext.getDataType() == Types.DATE) {
					return 10;
				} else {
					return 1;
				}
			}
		}

	    private boolean isPartitionKey(Column col) {
	    	if (stats!=null) {
	    		if (col instanceof DynamicColumn) {
	    			// for now do nothing...
	    			// but we should be able to deduce range from the column definition
	    			return false;
	    		} else {
		            if (stats.isPartitionTable(col.getTable())) {
		                PartitionInfo partition = stats.getPartitionInfo(col.getTable());
		                if (partition.isPartitionKey(col)) {
		                	return true;
		                }
		            }
	    		}
	    	}
	    	// else
	    	return false;
	    }
	}

	private boolean isFactDomain(AppContext ctx, DomainPK domainPk) {
		try {
			Cartography cartography = ProjectManager.INSTANCE.getCartography(ctx, domainPk.getParent());
			if (cartography != null) {
				return cartography.isFactDomain(domainPk);
			} else {
				return false;
			}
		} catch (ScopeException e) {
			return false;
		}
	}

	private ValueType computeValueType(IDomain image) {
		if (image.isInstanceOf(IDomain.STRING)) {
			return ValueType.STRING;
		} else if (image.isInstanceOf(IDomain.NUMERIC)) {
			return ValueType.NUMERIC;
		} else if (image.isInstanceOf(IDomain.TEMPORAL)) {
			return ValueType.DATE;
		} else if (image.isInstanceOf(IDomain.CONDITIONAL)) {
			return ValueType.CONDITION;
		} else if (image.isInstanceOf(IDomain.OBJECT)) {
			return ValueType.OBJECT;
		} else
			return ValueType.OTHER;
	}

	private String normalizeObjectName(String name) {
		return WordUtils.capitalizeFully(name,' ','-','_').replace('-',' ').replace('_', ' ').trim();
	}
    
    public String digest(String data) {
    	return org.apache.commons.codec.digest.DigestUtils.sha256Hex(data);
    }

	/**
	 * check if the dimension definition has been altered in any way
	 * 
	 * @param dimension
	 * @return
	 */
	public boolean isNatural(Dimension dimension) {
		String prefix = "dyn_" + dimension.getId().getParent().toUUID() + "_dimension:";
		String id = digest(prefix + dimension.getExpression().getValue());
		return id.equals(dimension.getId().getDimensionId());
	}

	public boolean isNatural(Metric metric) {
		String prefix = "dyn_" + metric.getId().getParent().toUUID() + "_metric:";
		String id = digest(prefix + metric.getExpression().getValue());
		return id.equals(metric.getId().getMetricId());
	}

}
