{{ objname }} *(class)*
{{ underline }}==========

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
   :members:
   :show-inheritance:
   :inherited-members:
   :member-order: groupwise

    {% block members %}

   {% if members %}
   .. rubric:: Methods

   .. autosummary::
   {% for item in methods %}
        {% if "__" not in item %}
            ~{{ name }}.{{ item }}
       {% endif %}
   {% endfor %}
   .. {% for item in members %}
   ..     {% if "__" not in item and item not in methods and "_" in item %}
   ..          ~{{ name }}.{{ item }}
   ..      {% endif %}
   .. {% endfor %}
   {% endif %}
   {% endblock %}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: Properties

   .. autosummary::
   {% for item in attributes %}
      ~{{ name }}.{{ item }}
   {% endfor %}
   {% endif %}
   {% endblock %}
