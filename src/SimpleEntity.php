<?php

namespace Assertis\SimpleDatabase;

use JsonSerializable;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
abstract class SimpleEntity implements JsonSerializable
{

    abstract protected function getAsArray();

    /**
     * @return array
     */
    public function toArray()
    {
        $out = [];
        foreach ($this->getAsArray() as $key => $value) {
            if (is_object($value) && method_exists($value, 'toArray')) {
                $out[$key] = $value->toArray();
            } else {
                $out[$key] = $value;
            }
        }
        return $out;
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return $this->toArray();
    }

}