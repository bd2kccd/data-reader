/*
 * Copyright (C) 2017 University of Pittsburgh.
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
package edu.pitt.dbmi.data.reader;

import java.io.IOException;
import java.util.List;

/**
 *
 * Feb 10, 2017 4:46:56 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public interface PreviewDataReader {

    public List<String> getPreviews(int fromRow, int toRow, int fromColumn, int toColumn) throws IOException;

    public void setQuoteCharacter(char quoteCharacter);

    public void setCommentMarker(String commentMarker);

}
