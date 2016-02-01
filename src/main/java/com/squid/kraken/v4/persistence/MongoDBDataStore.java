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
package com.squid.kraken.v4.persistence;

import java.util.Collection;
import java.util.List;

import org.mongodb.morphia.query.Query;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;

public class MongoDBDataStore implements DataStore {

	public MongoDBDataStore() {
	}

	public <T extends Persistent<PK>, PK extends GenericPK> T create(
			AppContext ctx, T object) {
		MongoDBHelper.getDatastore().save(object);
		return object;
	}

	@Override
	public <T extends Persistent<PK>, PK extends GenericPK> void delete(
			AppContext ctx, Class<T> type, PK id) {
		MongoDBHelper.getDatastore().delete(type, id.toUUID());
	}

	@Override
	public <T extends Persistent<PK>, PK extends GenericPK> boolean exists(
			AppContext ctx, Class<T> type, PK id) {
		return (MongoDBHelper.getDatastore().get(type, id.toUUID()) != null);
	}

	@Override
	public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(
			AppContext app, Class<T> type,
			List<DataStoreQueryField> queryFields,
			List<DataStoreFilterOperator> filterOperators, String orderBy) {
		List<T> list;
		// build the query
		Query<T> query = MongoDBHelper.getDatastore().createQuery(type);
		if (queryFields != null) {
			if (filterOperators == null) {
				for (DataStoreQueryField field : queryFields) {
					query = query.field(field.getName())
							.equal(field.getValue());
				}
			} else {
				if (queryFields.size() != filterOperators.size()) {
					throw new APIException(
							"The size of queryFields and filterOperators must be the same",
							app.isNoError());
				}
				DataStoreFilterOperator operator;
				int index = 0;
				for (DataStoreQueryField field : queryFields) {
					operator = filterOperators.get(index);
					switch (operator) {
					case EQUAL:
						query = query.field(field.getName()).equal(
								field.getValue());
						break;
					case ALL:
						query = query.field(field.getName()).hasAllOf(
								(Collection<?>) field.getValue());
						break;
					case GREATER_THAN:
						query = query.field(field.getName()).greaterThan(
								field.getValue());
						break;
					case GREATER_THAN_OR_EQUAL:
						query = query.field(field.getName()).greaterThanOrEq(
								field.getValue());
						break;
					case LESS_THAN:
						query = query.field(field.getName()).lessThan(
								field.getValue());
						break;
					case LESS_THAN_OR_EQUAL:
						query = query.field(field.getName()).lessThanOrEq(
								field.getValue());
						break;
					case NOT_EQUAL:
						query = query.field(field.getName()).notEqual(
								field.getValue());
						break;
					case IN:
						query = query.field(field.getName()).in(
								(Collection<?>) field.getValue());
						break;
					case NOT_IN:
						query = query.field(field.getName()).notIn(
								(Collection<?>) field.getValue());
						break;
					case STARTS_WITH:
						query = query.field(field.getName()).startsWith(
								field.getValue().toString());
						break;
					default:
						throw new APIException("Unknown operator '" + operator
								+ "'", app.isNoError());
					}
					index++;
				}
			}
		}
		// execute the query
		if ((orderBy != null) && !orderBy.isEmpty()) {
			query.order(orderBy);
		}
		list = query.asList();

		return list;
	}

	@Override
	public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(
			AppContext app, Class<T> type,
			List<DataStoreQueryField> queryFields,
			List<DataStoreFilterOperator> filterOperators) {
		return find(app, type, queryFields, filterOperators, null);
	}

	@Override
	public <T extends Persistent<PK>, PK extends GenericPK> Optional<T> read(
			AppContext ctx, Class<T> type, PK id) {
		T object = MongoDBHelper.getDatastore().get(type, id.toUUID());
		if (object != null) {
			return Optional.of(object);
		} else {
			return Optional.absent();
		}
	}

	@Override
	public <T extends Persistent<PK>, PK extends GenericPK> T readNotNull(
			AppContext ctx, Class<T> type, PK id) {
		Optional<T> object = read(ctx, type, id);
		if (object.isPresent()) {
			return object.get();
		} else {
			throw new ObjectNotFoundAPIException("Object not found with id : "
					+ id, ctx.isNoError());
		}
	}

	@Override
	public <T extends Persistent<PK>, PK extends GenericPK> void update(
			AppContext ctx, T transientObject) {
		MongoDBHelper.getDatastore().save(transientObject);
	}

}
