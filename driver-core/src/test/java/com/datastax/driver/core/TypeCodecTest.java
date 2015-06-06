/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.text;
import static com.datastax.driver.core.DataType.varchar;

public class TypeCodecTest {

    public static final DataType CUSTOM_FOO = DataType.custom("com.example.FooBar");

    private CodecRegistry codecRegistry = CodecRegistry.builder()
        .withDefaultCodecs()
        .withCustomType(CUSTOM_FOO).build();

    @Test(groups = "unit")
    public void testCustomList() throws Exception {
        DataType cqlType = DataType.list(CUSTOM_FOO);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().canDeserialize(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomSet() throws Exception {
        DataType cqlType = DataType.set(CUSTOM_FOO);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().canDeserialize(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomKeyMap() throws Exception {
        DataType cqlType = DataType.map(CUSTOM_FOO, text());
        TypeCodec<Map<?, ?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().canDeserialize(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomValueMap() throws Exception {
        DataType cqlType = DataType.map(text(), CUSTOM_FOO);
        TypeCodec<Map<?, ?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().canDeserialize(cqlType);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.cint());
        List<Integer> list = Collections.nCopies(65536, 1);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionElementTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.text());
        List<String> list = Lists.newArrayList(Strings.repeat("a", 65536));
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list);
    }

    @Test(groups = "unit")
    public void test_cql_text_to_json() {
        TextToJsonCodec codec = new TextToJsonCodec();
        String json = "{\"id\":1,\"name\":\"John Doe\"}";
        User user = new User(1, "John Doe");
        assertThat(codec.format(user)).isEqualTo(json);
        assertThat(codec.parse(json)).isEqualToComparingFieldByField(user);
        assertThat(codec.deserialize(codec.serialize(user))).isEqualToComparingFieldByField(user);
    }

    @Test(groups = "unit")
    public void test_cql_list_varchar_to_list_list_integer() {
        ListVarcharToListListInteger codec = new ListVarcharToListListInteger();
        List<List<Integer>> list = new ArrayList<List<Integer>>();
        list.add(Lists.newArrayList(1,2,3));
        list.add(Lists.newArrayList(4,5,6));
        assertThat(codec.deserialize(codec.serialize(list))).isEqualTo(list);
    }

    private class TextToJsonCodec extends TypeCodec<User> {

        private final TextCodec codec = TextCodec.instance;

        private final ObjectMapper objectMapper = new ObjectMapper();

        protected TextToJsonCodec() {
            super(text(), User.class);
        }

        @Override
        public ByteBuffer serialize(User value) {
            return codec.serialize(format(value));
        }

        @Override
        public User deserialize(ByteBuffer bytes) {
            return parse(codec.deserialize(bytes));
        }

        @Override
        public User parse(String value) {
            try {
                return objectMapper.readValue(value, User.class);
            } catch (IOException e) {
                throw new InvalidTypeException(e.getMessage(), e);
            }
        }

        @Override
        public String format(User value) {
            try {
                return objectMapper.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                throw new InvalidTypeException(e.getMessage(), e);
            }
        }
    }

    private class ListVarcharToListListInteger extends TypeCodec<List<List<Integer>>> {

        private final ListCodec<String> codec = new ListCodec<String>(VarcharCodec.instance);

        protected ListVarcharToListListInteger() {
            super(list(varchar()), listOf(listOf(Integer.class)));
        }

        @Override
        public ByteBuffer serialize(List<List<Integer>> value) {
            return codec.serialize(Lists.transform(value, new Function<List<Integer>, String>() {
                @Override
                public String apply(List<Integer> input) {
                    return Joiner.on(",").join(input);
                }
            }));
        }

        @Override
        public List<List<Integer>> deserialize(ByteBuffer bytes) {
            return Lists.transform(codec.deserialize(bytes), new Function<String, List<Integer>>() {
                @Override
                public List<Integer> apply(String input) {
                    return Lists.transform(Arrays.asList(input.split(",")), new Function<String, Integer>() {
                        @Override
                        public Integer apply(String input) {
                            return Integer.parseInt(input);
                        }
                    });
                }
            });
        }

        @Override
        public List<List<Integer>> parse(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(List<List<Integer>> value) {
            throw new UnsupportedOperationException();
        }
    }

    @SuppressWarnings("unused")
    public static class User {

        private int id;

        private String name;

        @JsonCreator
        public User(@JsonProperty("id")int id, @JsonProperty("name")String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
