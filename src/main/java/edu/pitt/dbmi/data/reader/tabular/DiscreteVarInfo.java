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
package edu.pitt.dbmi.data.reader.tabular;

import edu.pitt.dbmi.data.reader.tabular.TabularColumnFileReader.DataColumn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * Nov 12, 2018 3:45:32 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class DiscreteVarInfo {

    protected final DataColumn dataColumn;
    protected final Map<String, Integer> values;
    protected List<String> categories;

    public DiscreteVarInfo(DataColumn dataColumn) {
        this.dataColumn = dataColumn;
        this.values = new TreeMap<>();
    }

    @Override
    public String toString() {
        return "DiscreteVarInfo{" + "dataColumn=" + dataColumn + ", values=" + values + ", categories=" + categories + '}';
    }

    public void recategorize() {
        Set<String> keyset = values.keySet();
        categories = new ArrayList<>(keyset.size());
        int count = 0;
        for (String key : keyset) {
            values.put(key, count++);
            categories.add(key);
        }
    }

    public void setValue(String value) {
        this.values.put(value, null);
    }

    public Integer getEncodeValue(String value) {
        return values.get(value);
    }

    public DataColumn getDataColumn() {
        return dataColumn;
    }

    public List<String> getCategories() {
        return (categories == null) ? Collections.EMPTY_LIST : categories;
    }

}
