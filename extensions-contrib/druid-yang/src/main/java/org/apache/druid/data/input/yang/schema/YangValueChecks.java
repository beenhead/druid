/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.yang.schema;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.druid.java.util.common.StringUtils;
import org.yangcentral.yangkit.model.api.restriction.Binary;
import org.yangcentral.yangkit.model.api.restriction.Bits;
import org.yangcentral.yangkit.model.api.restriction.Decimal64;
import org.yangcentral.yangkit.model.api.restriction.Empty;
import org.yangcentral.yangkit.model.api.restriction.Enumeration;
import org.yangcentral.yangkit.model.api.restriction.IdentityRef;
import org.yangcentral.yangkit.model.api.restriction.InstanceIdentifier;
import org.yangcentral.yangkit.model.api.restriction.LeafRef;
import org.yangcentral.yangkit.model.api.restriction.Restriction;
import org.yangcentral.yangkit.model.api.restriction.Union;
import org.yangcentral.yangkit.model.api.restriction.YangBoolean;
import org.yangcentral.yangkit.model.api.restriction.YangInteger;
import org.yangcentral.yangkit.model.api.restriction.YangString;
import org.yangcentral.yangkit.model.api.stmt.Base;
import org.yangcentral.yangkit.model.api.stmt.Identity;
import org.yangcentral.yangkit.model.api.stmt.Type;
import org.yangcentral.yangkit.model.api.stmt.TypedDataNode;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Builds {@link YangSchemaIndex.ValueCheck}s from yangkit types, following the JSON encoding of RFC 7951. Ranges,
 * lengths, patterns, enums and bits are evaluated by yangkit's restrictions.
 *
 * <p>Integers are accepted both as JSON numbers and as strings, whatever their size: RFC 7951 asks for strings only
 * for 64-bit types, but producers differ. Leafrefs are checked against the type of the leaf they point to, not
 * against the instance data. If yangkit cannot evaluate a restriction, the value is accepted.
 */
class YangValueChecks
{
  private static final int MAX_LEAFREF_DEPTH = 8;
  private static final int MAX_SHOWN_LENGTH = 64;

  /**
   * Identities by {@code module:name}, for checking identityref values.
   */
  private final Map<String, Identity> identities;

  YangValueChecks(Map<String, Identity> identities)
  {
    this.identities = identities;
  }

  YangSchemaIndex.ValueCheck forType(@Nullable Type type)
  {
    return forType(type, 0);
  }

  private YangSchemaIndex.ValueCheck forType(@Nullable Type type, int leafrefDepth)
  {
    if (type == null) {
      return value -> null;
    }
    final String typeName = type.getArgStr();
    final Restriction<?> restriction = type.getRestriction();
    if (restriction instanceof LeafRef) {
      final TypedDataNode target = ((LeafRef) restriction).getReferencedNode();
      return target == null || leafrefDepth >= MAX_LEAFREF_DEPTH
             ? value -> null
             : forType(target.getType(), leafrefDepth + 1);
    } else if (restriction instanceof YangInteger) {
      return value -> checkInteger((YangInteger<?>) restriction, typeName, value);
    } else if (restriction instanceof Decimal64) {
      return value -> checkDecimal((Decimal64) restriction, typeName, value);
    } else if (restriction instanceof YangBoolean) {
      return value -> value.isBoolean() ? null : invalid(value, typeName);
    } else if (restriction instanceof Empty) {
      return value -> value.isArray() && value.size() == 1 && value.get(0).isNull() ? null : invalid(value, typeName);
    } else if (restriction instanceof Enumeration || restriction instanceof YangString) {
      return value -> value.isTextual() && evaluate(restriction, value.textValue()) ? null : invalid(value, typeName);
    } else if (restriction instanceof Bits) {
      return value -> checkBits((Bits) restriction, typeName, value);
    } else if (restriction instanceof Binary) {
      return value -> checkBinary((Binary) restriction, typeName, value);
    } else if (restriction instanceof IdentityRef) {
      return value -> checkIdentityRef((IdentityRef) restriction, typeName, value);
    } else if (restriction instanceof InstanceIdentifier) {
      return value -> value.isTextual() && value.textValue().startsWith("/") ? null : invalid(value, typeName);
    } else if (restriction instanceof Union) {
      final List<Type> members = ((Union) restriction).getActualTypes();
      if (members == null || members.isEmpty()) {
        return value -> null;
      }
      final List<YangSchemaIndex.ValueCheck> checks = new ArrayList<>(members.size());
      for (Type member : members) {
        checks.add(forType(member, leafrefDepth));
      }
      return value -> {
        for (YangSchemaIndex.ValueCheck check : checks) {
          if (check.check(value) == null) {
            return null;
          }
        }
        return invalid(value, typeName);
      };
    }
    return value -> null;
  }

  @Nullable
  private static String checkInteger(YangInteger<?> restriction, String typeName, JsonNode value)
  {
    final BigInteger number;
    if (value.isIntegralNumber()) {
      number = value.bigIntegerValue();
    } else if (value.isTextual()) {
      try {
        number = new BigInteger(value.textValue());
      }
      catch (NumberFormatException e) {
        return invalid(value, typeName);
      }
    } else {
      return invalid(value, typeName);
    }
    final Object min = restriction.getMin();
    final Object max = restriction.getMax();
    if (min == null || max == null) {
      return null;
    }
    if (number.compareTo(new BigInteger(min.toString())) < 0 || number.compareTo(new BigInteger(max.toString())) > 0) {
      return outOfRange(value, typeName);
    }
    // The restriction evaluates the boxed type of its built-in type (Byte for int8, BigInteger for uint64, ...).
    final Object boxed;
    if (min instanceof Byte) {
      boxed = number.byteValue();
    } else if (min instanceof Short) {
      boxed = number.shortValue();
    } else if (min instanceof Integer) {
      boxed = number.intValue();
    } else if (min instanceof Long) {
      boxed = number.longValue();
    } else {
      boxed = number;
    }
    return evaluate(restriction, boxed) ? null : outOfRange(value, typeName);
  }

  @Nullable
  private static String checkDecimal(Decimal64 restriction, String typeName, JsonNode value)
  {
    final BigDecimal number;
    if (value.isNumber()) {
      number = value.decimalValue();
    } else if (value.isTextual()) {
      try {
        number = new BigDecimal(value.textValue());
      }
      catch (NumberFormatException e) {
        return invalid(value, typeName);
      }
    } else {
      return invalid(value, typeName);
    }
    if (restriction.getEffectiveFractionDigits() != null
        && number.stripTrailingZeros().scale() > restriction.getEffectiveFractionDigits().getValue()) {
      return invalid(value, typeName);
    }
    return evaluate(restriction, number) ? null : outOfRange(value, typeName);
  }

  @Nullable
  private static String checkBits(Bits restriction, String typeName, JsonNode value)
  {
    if (!value.isTextual()) {
      return invalid(value, typeName);
    }
    final String text = value.textValue().trim();
    if (text.isEmpty()) {
      // No bits set; yangkit rejects the empty list.
      return null;
    }
    return evaluate(restriction, Arrays.asList(text.split("\\s+"))) ? null : invalid(value, typeName);
  }

  @Nullable
  private static String checkBinary(Binary restriction, String typeName, JsonNode value)
  {
    if (!value.isTextual()) {
      return invalid(value, typeName);
    }
    final byte[] bytes;
    try {
      bytes = Base64.getDecoder().decode(value.textValue());
    }
    catch (IllegalArgumentException e) {
      return invalid(value, typeName);
    }
    return evaluate(restriction, bytes) ? null : invalid(value, typeName);
  }

  /**
   * RFC 7951 section 6.8: {@code module:identity}, where the prefix may be left out if the identity is defined in the
   * leaf's module. Without the leaf's module at hand, an unprefixed name matches an identity of that name in any
   * module.
   */
  @Nullable
  private String checkIdentityRef(IdentityRef restriction, String typeName, JsonNode value)
  {
    if (!value.isTextual()) {
      return invalid(value, typeName);
    }
    final String text = value.textValue();
    final List<Identity> candidates = new ArrayList<>();
    if (text.indexOf(':') >= 0) {
      final Identity identity = identities.get(text);
      if (identity != null) {
        candidates.add(identity);
      }
    } else {
      for (Map.Entry<String, Identity> entry : identities.entrySet()) {
        if (entry.getKey().endsWith(":" + text)) {
          candidates.add(entry.getValue());
        }
      }
    }
    if (candidates.isEmpty()) {
      return StringUtils.format("unknown identity [%s]", shorten(text));
    }
    final List<Base> bases = restriction.getEffectiveBases();
    if (bases == null || bases.isEmpty()) {
      return null;
    }
    for (Identity candidate : candidates) {
      for (Base base : bases) {
        try {
          if (base.getIdentity() == null || candidate.isDerived(base.getIdentity())) {
            return null;
          }
        }
        catch (RuntimeException e) {
          return null;
        }
      }
    }
    return StringUtils.format("identity [%s] is not derived from the base of %s", shorten(text), typeName);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static boolean evaluate(Restriction restriction, Object value)
  {
    try {
      return restriction.evaluate(value);
    }
    catch (RuntimeException e) {
      return true;
    }
  }

  private static String invalid(JsonNode value, String typeName)
  {
    return StringUtils.format("invalid value %s for type %s", shorten(value.toString()), typeName);
  }

  private static String outOfRange(JsonNode value, String typeName)
  {
    return StringUtils.format("value %s is out of range for type %s", shorten(value.toString()), typeName);
  }

  private static String shorten(String text)
  {
    return text.length() <= MAX_SHOWN_LENGTH ? text : text.substring(0, MAX_SHOWN_LENGTH) + "...";
  }
}
