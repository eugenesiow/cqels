import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;


public class SelectAssertListener implements ContinuousListener, Callable<List<Mapping>> {

    private final List<Mapping> mapping = Collections.synchronizedList(
            new ArrayList<Mapping>());

    @Override
    public void update(Mapping mapping) {
        this.mapping.add(mapping);
    }

    @Override
    public List<Mapping> call() throws Exception {
        return mapping;
    }

}