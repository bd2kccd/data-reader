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
package edu.pitt.dbmi.data.validation;

import java.util.EnumMap;
import java.util.Map;

/**
 *
 * Feb 16, 2017 11:33:31 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class ValidationResult {

    private ValidationCode code;

    private String message;

    private final Map<ValidationAttribute, Object> attributes;

    public ValidationResult(ValidationCode code) {
        this(code, null);
    }

    public ValidationResult(ValidationCode code, String message) {
        this(code, message, new EnumMap<>(ValidationAttribute.class));
    }

    private ValidationResult(ValidationCode code, String message, Map<ValidationAttribute, Object> attributes) {
        this.code = code;
        this.message = message;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "ValidationResult{" + "code=" + code + ", message=" + message + ", attributes=" + attributes + '}';
    }

    public ValidationCode getCode() {
        return code;
    }

    public void setCode(ValidationCode code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<ValidationAttribute, Object> getAttributes() {
        return attributes;
    }

    public void setAttribute(ValidationAttribute attribute, Object value) {
        this.attributes.put(attribute, value);
    }

}
