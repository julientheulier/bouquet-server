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
import java.util.Collection;
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
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Column;
import com.squid.core.database.model.ForeignKey;
import com.squid.core.database.model.Index;
import com.squid.core.database.model.KeyPair;
import com.squid.core.database.model.Table;
import com.squid.core.database.statistics.IDatabaseStatistics;
import com.squid.core.database.statistics.PartitionInfo;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.reference.Cardinality;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.RelationDirection;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.db.features.IMetadataForeignKeySupport;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.core.analysis.engine.cartography.Cartography;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.CyclicDependencyException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainContent;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.database.impl.DatasourceDefinition;
import com.squid.kraken.v4.core.expression.reference.ColumnDomainReference;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.expression.visitor.ExtractColumns;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Dimension;
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
import com.squid.kraken.v4.model.Dimension.Type;
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
				Set<AccessRight> accessRights = project.getAccessRights();
				//
				HashSet<String> checkIDs = new HashSet<String>();// check IDs to
																	// avoid
																	// duplicates
																	// which
																	// will
																	// cause
																	// pain
				for (Domain domain : domains) {
					checkIDs.add(domain.getId().getDomainId());
					if (domain.getSubject() != null) {
						try {
							coverage.put(root.getTable(domain), domain);
						} catch (ScopeException e) {
							// ignore only if error is scope exception
							// if other exception, make sure that DB exception
							// will hit the user
						}
					}

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
						DomainPK domainPk = new DomainPK(project.getId(), checkUniqueId(tableRef, checkIDs));
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
		for (Domain domain : domains) {
			try {
				if (!domain.getOptions().getReinjected() && !domain.getOptions().getAlink()) {
					Table table = root.getTable(domain);
					HashMap<String, NaturalRelation> dedupLinks = new HashMap<>();
					for (ForeignKey fk : getForeignKeys(table)) {
						Table targetTable = fk.getPrimaryTable();
						Domain target = coverage.get(targetTable);
						if (target != null) {
							// create a relation ?
							String link = genRelationLinkId(project, domain, target);
							String id = link + ":" + fk.getName();
							String digest = digest(id);
							Relation relation = null;
							NaturalRelation samesame = null;
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
								relation = new Relation(relationPk, domain.getId(), Cardinality.MANY, target.getId(),
										Cardinality.ZERO_OR_ONE, leftName, rightName,
										new Expression("'" + fk.getName() + "'"), true);
								AccessRightsUtils.getInstance().setAccessRights(root.getContext(), relation, project);
								try {
									ExpressionAST check = root.getParser().parse(relation);
									if (!dedup.contains(check)) {
										relations.add(relation);
									}
								} catch (ScopeException e) {
									// ignore if invalid
								}
							}
							// always handle dedup
							dedupLinks.put(link,
									new NaturalRelation(relation, fk, relation != null && samesame == null));
						}
					}
				} else {
					// Use linked source or origin sources.
					domain.getOptions().getLinkSource();
					// Table table = root.getTable(domain);
					// for (ForeignKey fk : getForeignKeys(table)) {
					// Table targetTable = fk.getPrimaryTable();
					// Domain target = coverage.get(targetTable);
					// if (target != null) {
					// // create a relation ?
					// String id = "rel/" + domain.getId().toUUID() + "-" +
					// target.getId().toUUID() + ":" + fk.getName();
					// RelationPK relationPk = new RelationPK(project.getId(),
					// digest(id));
					// Relation relation =
					// new Relation(relationPk,
					// domain.getId(),
					// Cardinality.MANY,
					// target.getId(),
					// Cardinality.ZERO_OR_ONE,
					// domain.getName(),
					// target.getName(),
					// new Expression("'" + fk.getName() + "'"), true);
					// relation.setAccessRights(accessRights);
					// try {
					// ExpressionAST check = root.getParser().parse(relation);
					// if (!dedup.contains(check)) {
					// relations.add(relation);
					// }
					// } catch (ScopeException e) {
					// // ignore if invalid
					// }
					// }
					// }

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
		Universe univ = space.getUniverse();
		Domain domain = space.getDomain();
		// check if the domain is in legacy mode (i.e. default is to hide dynamic)
		boolean isDomainLegacyMode = domain.getInternalVersion() == null;
		// domainInternalDefautDynamic flag: if legacy mode, hide dynamic object is the default
		boolean domainInternalDefautDynamic = isDomainLegacyMode ? true : false;
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
		// sort by level (0 first, ...)
		List<ExpressionObject<?>> concrete = new ArrayList<ExpressionObject<?>>();
		concrete.addAll(content.getDimensions());
		concrete.addAll(content.getMetrics());
		Collections.sort(concrete, new LevelComparator<ExpressionObject<?>>());
		// failed List : keep track of failed evaluation, will try again latter
		List<ExpressionObject<?>> failed = new ArrayList<ExpressionObject<?>>();
		for (ExpressionObject<?> object : concrete) {
			if (object.getName() != null) {
				checkName.add(object.getName());
			}
			if (object instanceof Dimension) {
				// handle Dimension
				Dimension dimension = (Dimension) object;
				try {
					if (dimension.getId() != null) {
						ids.add(dimension.getId().getDimensionId());
						// add also the canonical ID
						if (dimension.getExpression() != null && dimension.getExpression().getValue() != null) {
							ids.add(digest(prefix + dimension.getExpression().getValue()));
						}
						// add also the Axis ID
						ids.add(space.A(dimension).getId());
					}
					ExpressionAST expr = parseResilient(univ, domain, dimension, scope);
					scope.add(object);
					IDomain image = expr.getImageDomain();
					dimension.setImageDomain(image);
					dimension.setValueType(computeValueType(image));
					if (expr instanceof ColumnReference) {
						ColumnReference ref = (ColumnReference) expr;
						if (ref.getColumn() != null) {
							coverage.add(ref.getColumn());
						}
					} else if (image.isInstanceOf(IDomain.OBJECT)) {
						// it's an sub-domain, we build the space to connect and
						// will dedup for dynamics
						Space path = space.S(expr);
						neighborhood.add(path);
					}
					if (dimension.getType() == Type.CONTINUOUS && image.isInstanceOf(IDomain.TEMPORAL)) {
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
						ExpressionAST expr = parseResilient(univ, domain, metric, scope);
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
		//
		// exclude keys
		HashSet<Column> keys = new HashSet<Column>();
		// filter out the primary-key
		try {
			Index pk = space.getTable().getPrimaryKey();
			if (pk != null) {
				for (Column col : pk.getColumns()) {
					keys.add(col);
				}
			}
		} catch (ScopeException e) {
			// ignore
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
			if (!keys.contains(col) && !coverage.contains(col) && includeColumnAsDimension(col)) {
				ColumnReference ref = new ColumnReference(col);
				String expr = ref.prettyPrint();
				DimensionPK id = new DimensionPK(domain.getId(), digest(prefix + expr));
				if (!ids.contains(id.getDimensionId())) {
					Type type = Type.INDEX;
					String name = checkName(normalizeObjectName(col.getName()), checkName);
					Dimension dim = new Dimension(id, name, type, new Expression(expr), domainInternalDefautDynamic);
					if (col.getDescription()!=null) dim.setDescription(col.getDescription());
					dim.setImageDomain(col.getTypeDomain());
					dim.setValueType(computeValueType(col.getTypeDomain()));
					AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), dim, domain);
					content.add(dim);
					checkName.add(name);
					if (col.getTypeDomain().isInstanceOf(IDomain.TEMPORAL) && !isPeriodDefined) {
						periodCandidates.add(new RawDImension(col, dim));
					}
				}
			}
		}
		// relation and FK
		for (Space neighbor : subspaces) {
			if (neighbor.length() == 1 // build only direct paths (the facet
										// will populate the others
					&& !neighborhood.contains(neighbor)) // dedup if already
															// concrete
															// associated with
															// the same path
			{
				Relation relation = neighbor.getRelation();
				try {
					RelationReference ref = new RelationReference(space.getUniverse(), relation, space.getDomain(),
							neighbor.getDomain());
					if (useRelation(relation, ref)) {
						checkName.add(ref.getReferenceName());
						String expr = ref.prettyPrint() + ".$'SELF'";// add the
																		// SELF
																		// parameter
						DimensionPK id = new DimensionPK(domain.getId(), digest(prefix + expr));
						if (!ids.contains(id.getDimensionId())) {
							String name = ref.getReferenceName();
							if (isDomainLegacyMode) {
								name = checkName(">" + name, checkName);
							} else {
								// this is the new naming convention for
								// sub-domains
								name = checkName(name + " > ", checkName);
							}
							Dimension dim = new Dimension(id, name, Type.INDEX, new Expression(expr),
									domainInternalDefautDynamic);
							dim.setDescription("relation to "+neighbor.getDomain().getName());
							dim.setValueType(ValueType.OBJECT);
							dim.setImageDomain(ref.getImageDomain());
							AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), dim, domain);
							content.add(dim);
							checkName.add(name);
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
		if (!coverage.contains(count)) {
			Expression expr = new Expression(count.prettyPrint());
			MetricPK metricId = new MetricPK(domain.getId(), digest(prefix + expr.getValue()));
			if (!ids.contains(metricId.getMetricId())) {// check for natural
														// definition
				String name = "COUNT " + domain.getName();
				name = checkName(name, checkName);
				Metric metric = new Metric(metricId, name, expr, domainInternalDefautDynamic);
				metric.setDescription(domain.getName() + " count");
				AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), metric, domain);
				content.add(metric);
				checkName.add(name);
			}
		}
		//
		for (Column col : columns) {
			if (col.getTypeDomain().isInstanceOf(IDomain.NUMERIC)) {
				if (!keys.contains(col)) {
					ExpressionAST total = ExpressionMaker.SUM(new ColumnDomainReference(space, col));
					if (!coverage.contains(total)) {
						Expression expr = new Expression(total.prettyPrint());
						MetricPK metricId = new MetricPK(domain.getId(), digest(prefix + expr.getValue()));
						if (!ids.contains(metricId.getMetricId())) {// check for
																	// natural
																	// definition
							String name = "SUM " + normalizeObjectName(col.getName());
							name = checkName(name, checkName);
							Metric metric = new Metric(metricId, name, expr, domainInternalDefautDynamic);
							if (col.getDescription()!=null) metric.setDescription(col.getDescription());
							AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), metric, domain);
							content.add(metric);
							checkName.add(name);
						}
					}
				}
			}
		}
		//
		// try to recover failed ones
		for (ExpressionObject<?> object : failed) {
			try {
				if (object.getExpression() != null) {
					if (object instanceof Dimension) {
						// handle Dimension
						Dimension dimension = (Dimension) object;
						ExpressionAST expr = parseResilient(univ, domain, dimension, scope);
						scope.add(object);
						IDomain image = expr.getImageDomain();
						dimension.setImageDomain(image);
						dimension.setValueType(computeValueType(image));
					} else if (object instanceof Metric) {
						// handle Metric
						Metric metric = (Metric) object;
						ExpressionAST expr = parseResilient(univ, domain, metric, scope);
						scope.add(object);
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
		boolean needPeriod = !isPeriodDefined // if already defined, that's fine
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

	private ExpressionAST parseResilient(Universe root, Domain domain, Dimension dimension,
			Collection<ExpressionObject<?>> scope) throws ScopeException {
		try {
			if (dimension.getExpression().getReferences() != null) {
				scope = new ArrayList<>(scope);
				for (Object safeCasting : dimension.getExpression().getReferences()) {
					if (safeCasting instanceof ReferencePK<?>) {
						ReferencePK<?> unwrap = (ReferencePK<?>) safeCasting;
						GenericPK ref = unwrap.getReference();
						Optional<? extends ExpressionObject<?>> value = findReference(root, ref);
						if (value.isPresent()) {
							scope.add(value.get());
						}
					}
				}
			}
			return root.getParser().parse(domain, dimension, scope);
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

	private ExpressionAST parseResilient(Universe root, Domain domain, Metric metric,
			Collection<ExpressionObject<?>> scope) throws ScopeException {
		try {
			if (metric.getExpression().getReferences() != null) {
				scope = new ArrayList<>(scope);
				for (Object safeCasting : metric.getExpression().getReferences()) {
					if (safeCasting instanceof ReferencePK<?>) {
						ReferencePK<?> unwrap = (ReferencePK<?>) safeCasting;
						GenericPK ref = unwrap.getReference();
						Optional<? extends ExpressionObject<?>> value = findReference(root, ref);
						if (value.isPresent()) {
							scope.add(value.get());
						}
					}
				}
			}
			return root.getParser().parse(domain, metric, scope);
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
			if (stats != null) {
				if (stats.isPartitionTable(col.getTable())) {
					PartitionInfo partition = stats.getPartitionInfo(col.getTable());
					if (partition.isPartitionKey(col)) {
						return true;
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
		return WordUtils.capitalizeFully(name, ' ', '-', '_').replace('-', ' ').replace('_', ' ');
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
