/*
 * Copyright (C) 2018 University of Pittsburgh.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package edu.pitt.dbmi.data.reader.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.pitt.dbmi.data.reader.metadata.interventional.InterventionalDataColumn;
import java.util.List;

/**
 *
 * Dec 18, 2018 1:25:21 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class DataColumnMetadata {

    @JsonProperty("interventions")
    private List<InterventionalDataColumn> interventionalDataColumns;

    @JsonProperty("domains")
    private List<DomainDataColumn> domainDataColumns;

    public DataColumnMetadata() {
    }

    public DataColumnMetadata(List<InterventionalDataColumn> interventionalDataColumns, List<DomainDataColumn> domainDataColumns) {
        this.interventionalDataColumns = interventionalDataColumns;
        this.domainDataColumns = domainDataColumns;
    }

    public List<InterventionalDataColumn> getInterventionalDataColumns() {
        return interventionalDataColumns;
    }

    public void setInterventionalDataColumns(List<InterventionalDataColumn> interventionalDataColumns) {
        this.interventionalDataColumns = interventionalDataColumns;
    }

    public List<DomainDataColumn> getDomainDataColumns() {
        return domainDataColumns;
    }

    public void setDomainDataColumns(List<DomainDataColumn> domainDataColumns) {
        this.domainDataColumns = domainDataColumns;
    }

}
