<?php

namespace Icinga\Module\Director\Objects;

class IcingaServiceapply extends IcingaService {

    public function createWhere()
    {
        $where = parent::createWhere();
        if (! $this->hasBeenLoadedFromDb()) {
            if (null === $this->get('service_set_id')
                && null === $this->get('host_id')
                && null === $this->get('id')
            ) {
                $where = str_replace(" AND object_type = 'template'", "", $where);
            }
        }

        return $where;
    }
}
