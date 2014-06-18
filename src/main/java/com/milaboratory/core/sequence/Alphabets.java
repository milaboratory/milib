package com.milaboratory.core.sequence;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class Alphabets {
    private final static Map<String, Alphabet> alphabets = new HashMap<>();

    private Alphabets() {
    }

    public static void register(Alphabet alphabet) {
        if (alphabets.put(alphabet.getAlphabetName(), alphabet) != null)
            throw new IllegalStateException();
    }

    static {
        register(NucleotideAlphabet.INSTANCE);
        register(AminoAcidAlphabet.INSTANCE);
        register(IncompleteAlphabet.INCOMPLETE_NUCLEOTIDE_ALPHABET);
        register(IncompleteAlphabet.INCOMPLETE_AMINO_ACID_ALPHABET);
    }

    public static Alphabet getByName(String name) {
        return alphabets.get(name);
    }

    public static final class Deserializer extends JsonDeserializer<Alphabet> {
        @Override
        public Alphabet deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return Alphabets.getByName(jp.readValueAs(String.class));
        }

        @Override
        public Alphabet getEmptyValue() {
            return NucleotideAlphabet.INSTANCE;
        }

        @Override
        public Alphabet getNullValue() {
            return NucleotideAlphabet.INSTANCE;
        }
    }

    public static final class Serializer extends JsonSerializer<Alphabet> {
        @Override
        public void serialize(Alphabet value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            jgen.writeString(value.getAlphabetName());
        }
    }
}
