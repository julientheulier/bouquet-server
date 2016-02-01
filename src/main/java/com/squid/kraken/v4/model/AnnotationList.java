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
package com.squid.kraken.v4.model;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * List of annotations of a project. This list will be returned when user calling WS that lists all the annotations
 * available.
 * 
 * @author danhddv
 * 
 */
@XmlRootElement
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
public class AnnotationList implements Serializable {

    /**
     * Last date when the annotation is read. It is just the milliseconds.
     */
    Long lastAnnotationReadTimestamp;

    /**
     * List of annotations of a project.
     */
    List<Annotation> annotations;

    /**
     * Get the "lastAnnotationReadTimestamp" variable.
     * 
     * @return the lastAnnotationReadTimestamp
     */
    public Long getLastAnnotationReadTimestamp() {
        return lastAnnotationReadTimestamp;
    }

    /**
     * Set the "lastAnnotationReadTimestamp" variable.
     * 
     * @param lastAnnotationReadTimestamp
     *            the lastAnnotationReadTimestamp to set
     */
    public void setLastAnnotationReadTimestamp(Long lastAnnotationReadTimestamp) {
        this.lastAnnotationReadTimestamp = lastAnnotationReadTimestamp;
    }

    /**
     * Get the "annotations" variable.
     * 
     * @return the annotations
     */
    public List<Annotation> getAnnotations() {
        return annotations;
    }

    /**
     * Set the "annotations" variable.
     * 
     * @param annotations
     *            the annotations to set
     */
    public void setAnnotations(List<Annotation> annotations) {
        this.annotations = annotations;
    }

}
