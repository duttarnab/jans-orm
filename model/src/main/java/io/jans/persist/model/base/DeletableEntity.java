package io.jans.persist.model.base;

import io.jans.orm.annotation.AttributeName;
import io.jans.orm.annotation.DataEntry;

import java.util.Date;

/**
 * @author Yuriy Zabrovarnyy
 */
@DataEntry
public class DeletableEntity extends BaseEntry implements Deletable {

    @AttributeName(name = "exp")
    private Date expirationDate;
    @AttributeName(name = "del")
    private Boolean deletable;

    @Override
    public Boolean isDeletable() {
        return deletable;
    }

    public void setDeletable(Boolean deletable) {
        this.deletable = deletable;
    }

    public Date getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }

	public boolean canDelete() {
        return canDelete(new Date());
    }

    public boolean canDelete(Date now) {
    	Date exp = expirationDate != null ? expirationDate :  null;
        return deletable != null && deletable && (exp == null || exp.before(now));
    }

    @Override
    public String toString() {
    	Date exp = expirationDate != null ? expirationDate :  null;
        return "DeletableEntity{" +
                "expirationDate=" + exp +
                ", deletable=" + deletable +
                "} " + super.toString();
    }
}
