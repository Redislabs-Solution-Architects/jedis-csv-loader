package com.jsd.jedis;


import ai.djl.ModelException;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;


import java.io.IOException;
import java.util.Arrays;

public class VectorEmbedding {


    private  ZooModel<String, float[]> loadHFModel() throws IOException, ModelException, TranslateException {
        Criteria<String, float[]> criteria = Criteria.builder()
                .setTypes(String.class, float[].class)
                .optModelUrls("djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2")
                .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
                .optEngine("PyTorch")
                .optProgress(new ProgressBar())
                .build();

                return criteria.loadModel();
    }





    public static void main(String[] args) throws IOException, ModelException, TranslateException {

        VectorEmbedding ve = new VectorEmbedding();

        ZooModel<String, float[]> model = ve.loadHFModel();
        Predictor<String, float[]> predictor = model.newPredictor();

        for(int i = 0; i < 2; i++) {
            float[] res = predictor.predict("how many states in USA " + i);
            System.out.println("=======================================");
            System.out.println("Embedding: " + res.length + ":\n" + Arrays.toString(res));
        }

    }
}