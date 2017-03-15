package com.squid.kraken.v4.model;

public interface HasVersionControl {
	
    /**
     * Version attribute used to implement optimistic-locking mechanism.
     */
    public Integer getVersionControl();
    
    public void setVersionControl(Integer version);
    
}