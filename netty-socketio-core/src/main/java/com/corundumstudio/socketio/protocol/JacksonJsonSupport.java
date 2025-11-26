/**
 * Copyright (c) 2012-2025 Nikita Koksharov
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.corundumstudio.socketio.protocol;

import com.corundumstudio.socketio.AckCallback;
import com.corundumstudio.socketio.MultiTypeAckCallback;
import com.corundumstudio.socketio.namespace.Namespace;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.std.StdSerializer;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public class JacksonJsonSupport implements JsonSupport {

    private final ThreadLocal<String> namespaceClass = new ThreadLocal<>();

    private final ThreadLocal<AckCallback<?>> currentAckClass = new ThreadLocal<>();

    /**
     * 用于跟踪字节数组的线程局部变量
     */
    private final ThreadLocal<List<byte[]>> binaryArrays = ThreadLocal.withInitial(ArrayList::new);

    private final JsonMapper jsonMapper;

    private final EventDeserializer eventDeserializer;

    private final AckArgsDeserializer ackArgsDeserializer;

    protected static final Logger log = LoggerFactory.getLogger(JacksonJsonSupport.class);

    public JacksonJsonSupport() {
        this(List.of());
    }

    public JacksonJsonSupport(@NonNull List<JacksonModule> modules) {
        // Jackson 3.x 使用 JsonMapper.builder() 创建 ObjectMapper
        JsonMapper.Builder builder = JsonMapper.builder();

        // 配置序列化/反序列化设置 - 使用 Jackson 3.x 的 API
        builder.changeDefaultPropertyInclusion(incl -> incl.withValueInclusion(NON_NULL));
        // 配置序列化/反序列化设置
        builder.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        builder.configure(tools.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        // 关键：禁用尾随令牌检查
        builder.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, false);
        // 创建自定义模块
        SimpleModule serializeModule = new SimpleModule();
        // 注册字节数组序列化器
        serializeModule.addSerializer(byte[].class, new ByteArraySerializer(binaryArrays::get));
        this.eventDeserializer = new EventDeserializer();
        this.ackArgsDeserializer = new AckArgsDeserializer();
        SimpleModule deserializerModule = new SimpleModule();
        deserializerModule.addDeserializer(Event.class, eventDeserializer);
        deserializerModule.addDeserializer(AckArgs.class, ackArgsDeserializer);
        // 注册内部模块
        List.of(serializeModule, deserializerModule).forEach(builder::addModule);
        // 注册外部模块
        modules.forEach(builder::addModule);
        this.jsonMapper = builder.build();
    }

    @Override
    public void addEventMapping(String namespaceName, String eventName, Class<?>... eventClass) {
        eventDeserializer.eventMapping.put(new EventKey(namespaceName, eventName), Arrays.asList(eventClass));
    }

    @Override
    public void removeEventMapping(String namespaceName, String eventName) {
        eventDeserializer.eventMapping.remove(new EventKey(namespaceName, eventName));
    }

    @Override
    public <T> T readValue(String namespaceName, ByteBufInputStream src, Class<T> valueType) {
        namespaceClass.set(namespaceName);
        try {
            return jsonMapper.readValue((InputStream) src, valueType);
        } finally {
            namespaceClass.remove();
        }
    }

    @Override
    public AckArgs readAckArgs(ByteBufInputStream src, AckCallback<?> callback) {
        currentAckClass.set(callback);
        try {
            return jsonMapper.readValue((InputStream) src, AckArgs.class);
        } finally {
            currentAckClass.remove();
        }
    }

    @Override
    public void writeValue(ByteBufOutputStream out, Object value) {
        // 清空线程局部的字节数组列表
        binaryArrays.remove();
        jsonMapper.writeValue((OutputStream) out, value);
    }

    @Override
    public List<byte[]> getArrays() {
        return binaryArrays.get();
    }

    public String getNamespaceClass() {
        return namespaceClass.get();
    }

    public AckCallback<?> getCurrentAckClass() {
        return currentAckClass.get();
    }


    public JsonMapper getJsonMapper() {
        return jsonMapper;
    }

    public EventDeserializer getEventDeserializer() {
        return eventDeserializer;
    }

    public AckArgsDeserializer getAckArgsDeserializer() {
        return ackArgsDeserializer;
    }

    private class AckArgsDeserializer extends StdDeserializer<AckArgs> {

        protected AckArgsDeserializer() {
            super(AckArgs.class);
        }

        @Override
        public AckArgs deserialize(JsonParser jp, DeserializationContext ctxt) throws JacksonException {
            List<Object> args = new ArrayList<>();
            AckArgs result = new AckArgs(args);
            ObjectReadContext objectReadContext = jp.objectReadContext();
            JsonNode root = objectReadContext.readTree(jp);
            AckCallback<?> callback = currentAckClass.get();
            Iterator<JsonNode> iter = root.iterator();
            int i = 0;
            while (iter.hasNext()) {
                Object val;
                Class<?> clazz = callback.getResultClass();
                if (callback instanceof MultiTypeAckCallback ackCallback) {
                    clazz = ackCallback.getResultClasses()[i];
                }

                JsonNode arg = iter.next();
                if (arg.isString() || arg.isBoolean()) {
                    clazz = Object.class;
                }
                val = objectReadContext.readValue(objectReadContext.treeAsTokens(arg), clazz);
                args.add(val);
                i++;
            }
            return result;
        }
    }

    public record EventKey(String namespaceName, String eventName) {

    }

    private class EventDeserializer extends StdDeserializer<Event> {

        private final Map<EventKey, List<Class<?>>> eventMapping = new ConcurrentHashMap<>();

        protected EventDeserializer() {
            super(Event.class);
        }

        @Override
        public Event deserialize(JsonParser jp, DeserializationContext ctxt) throws JacksonException {
            String eventName = jp.nextStringValue();
            EventKey ek = new EventKey(namespaceClass.get(), eventName);
            if (!eventMapping.containsKey(ek)) {
                ek = new EventKey(Namespace.DEFAULT_NAME, eventName);
                if (!eventMapping.containsKey(ek)) {
                    return new Event(eventName, Collections.emptyList());
                }
            }

            List<Object> eventArgs = new ArrayList<>();
            Event event = new Event(eventName, eventArgs);
            List<Class<?>> eventClasses = eventMapping.get(ek);
            int i = 0;
            while (true) {
                JsonToken token = jp.nextToken();
                if (token == JsonToken.END_ARRAY) {
                    break;
                }
                if (i > eventClasses.size() - 1) {
                    log.debug("Event {} has more args than declared in handler: {}", eventName, null);
                    break;
                }
                Class<?> eventClass = eventClasses.get(i);
                Object arg = jp.objectReadContext().readValue(jp, eventClass);
                eventArgs.add(arg);
                i++;
            }
            return event;
        }
    }

    // 简化的字节数组序列化器
    private static class ByteArraySerializer extends StdSerializer<byte[]> {

        private final Supplier<List<byte[]>> arrays;

        public ByteArraySerializer(Supplier<List<byte[]>> arrays) {
            super(byte[].class);
            this.arrays = arrays;
        }

        @Override
        public void serialize(byte[] value, JsonGenerator gen, SerializationContext provider) throws JacksonException {
            Map<String, Object> map = HashMap.newHashMap(4);
            map.put("num", arrays.get().size());
            map.put("_placeholder", true);
            gen.writePOJO(map);
            arrays.get().add(value);
        }
    }
}