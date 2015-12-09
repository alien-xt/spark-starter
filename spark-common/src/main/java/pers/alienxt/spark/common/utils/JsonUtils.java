package pers.alienxt.spark.common.utils;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * <p>文件描述：JsonUtils</p>
 * <p>内容摘要：Jackson转换工具类</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午11:28
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER;

    public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static {
        MAPPER = getGenerateMapper();
    }

    private JsonUtils() { }

    /**
     * 日期序列化格式化
     */
    public static class JsonDateSerializer extends JsonSerializer<Date> {

        @Override
        public void serialize(final Date value, final JsonGenerator jgen,
                final SerializerProvider provider) throws IOException {
            String date = SIMPLE_DATE_FORMAT.format(value);
            jgen.writeString(date);
        }
    }

    /**
     * 获取通用的mapper
     *
     * @return mapper
     */
    private static ObjectMapper getGenerateMapper() {
        ObjectMapper customMapper = new ObjectMapper();
        // 设置输入时忽略在JSON字符串中存在但Java对象实际没有的属�?
        customMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 禁止使用int代表Enum的order()來反序列化Enum
        customMapper.configure(DeserializationConfig.Feature.FAIL_ON_NUMBERS_FOR_ENUMS, true);
        // �?有日期格式都统一为以下样�?
        customMapper.setDateFormat(SIMPLE_DATE_FORMAT);
        customMapper.enableDefaultTyping();
        return customMapper;
    }

    /**
     * 字符串转对象
     *
     * @param json 字符�?
     * @param clazz �?
     * @param <T> 范型
     * @return 转化后的对象
     * @throws IOException 转换异常
     */
    public static <T> T fromJson(final String json, final Class<T> clazz)
            throws IOException {
        return clazz.equals(String.class) ? (T) json : MAPPER.readValue(json,
                clazz);
    }

    /**
     * 字符串转对象
     *
     * @param json 字符�?
     * @param typeReference 范型
     * @param <T> 范型
     * @return 转化后的对象
     * @throws IOException 转换异常
     */
    public static <T> T fromJson(final String json, final TypeReference<?> typeReference)
            throws IOException {
        return (T) (typeReference.getType().equals(String.class) ? json
                : MAPPER.readValue(json, typeReference));
    }

    /**
     * 对象转字符串
     *
     * @param src 对象
     * @param <T> 范型
     * @return 转化后的字符�?
     * @throws IOException 转换异常
     */
    public static <T> String toJson(final T src) throws IOException {
        return src instanceof String ? (String) src : MAPPER
                .writeValueAsString(src);
    }

    /**
     * 对象转字符串
     *
     * @param src 对象
     * @param inclusion 序列化设�?
     * @param <T> 范型
     * @return 转化后的字符�?
     * @throws IOException 转换异常
     */
    public static <T> String toJson(final T src, final JsonSerialize.Inclusion inclusion)
            throws IOException {
        if (src instanceof String) {
            return (String) src;
        } else {
            // 用户设置的序列化设置
            MAPPER.setSerializationInclusion(inclusion);
            return MAPPER.writeValueAsString(src);
        }
    }

    /**
     * 对象转字符串
     *
     * @param src 对象
     * @param mapper 自定义的mapper
     * @param <T> 范型
     * @return 转化后的字符�?
     * @throws IOException 转换异常
     */
    public static <T> String toJson(final T src, final ObjectMapper mapper)
            throws IOException {
        if (null != mapper) {
            if (src instanceof String) {
                return (String) src;
            } else {
                return mapper.writeValueAsString(src);
            }
        } else {
            return null;
        }
    }

    /**
     * 字符串转map
     *
     * @param src 字符�?
     * @return 转换后的map
     */
    public static Map json2Map(final String src) throws IOException {
        final Map<?, ?> map = MAPPER.readValue(src, Map.class);
        return map;
    }

    /**
     * json数组转java集合
     *
     * @param jsonArray json数组
     * @param cls �?
     * @param <T> 范型
     * @return 集合
     */
    public static <T> List<T> jsonArray2list(final String jsonArray, final Class<T> cls) {
        final List<T> ret = new ArrayList<T>();
        List<LinkedHashMap<String, Object>> list = null;
        try {
            list = MAPPER.readValue(jsonArray, List.class);
        } catch (JsonParseException e) {
            e.printStackTrace();
            return ret;
        } catch (JsonMappingException e) {
            e.printStackTrace();
            return ret;
        } catch (IOException e) {
            e.printStackTrace();
            return ret;
        }
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> map = list.get(i);
            try {
                String json = toJson(map);
                T obj = fromJson(json, cls);
                ret.add(obj);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
        }
        return ret;
    }
}
