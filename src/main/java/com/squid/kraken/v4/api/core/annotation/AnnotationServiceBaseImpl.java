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
package com.squid.kraken.v4.api.core.annotation;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.model.Annotation;
import com.squid.kraken.v4.model.AnnotationPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.dao.AnnotationDAO;

public class AnnotationServiceBaseImpl extends GenericServiceImpl<Annotation, AnnotationPK> {

    private static AnnotationServiceBaseImpl instance;

    public static AnnotationServiceBaseImpl getInstance() {
        if (instance == null) {
            instance = new AnnotationServiceBaseImpl();
        }
        return instance;
    }

    private AnnotationServiceBaseImpl() {
        // made private for singleton access
        super(Annotation.class);
    }

    @Override
    public Annotation store(AppContext ctx, Annotation newAnnotation) {
        // only message, annotation date can be modified
        AnnotationDAO annotationDao = (AnnotationDAO) factory.getDAO(Annotation.class);
        Optional<Annotation> opAnnotation = annotationDao.read(ctx, newAnnotation.getId());
        if (opAnnotation.isPresent()) {
            Annotation oldAnnotation = opAnnotation.get();
            if (oldAnnotation != null) {
                oldAnnotation.setMessage(newAnnotation.getMessage());
                Long annotationTimestamp = newAnnotation.getAnnotationTimestamp();
                if (annotationTimestamp != null) {
                    oldAnnotation.setAnnotationTimestamp(annotationTimestamp);
                } else {
                    oldAnnotation.setAnnotationTimestamp(oldAnnotation.getCreationTimestamp());
                }
                return super.store(ctx, oldAnnotation);
            }
        }
        return super.store(ctx, newAnnotation);
    }

}
